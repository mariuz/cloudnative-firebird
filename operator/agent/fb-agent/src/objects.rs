//! `fb-agent objects`: the operator's object store operations over the stores' own APIs
//! (docs/object-stores.md), with Apache Arrow's object_store: S3, Azure Blob and GCS.
//!
//!   fb-agent objects list <prefix>        the names of the objects under the prefix, one per line
//!   fb-agent objects put <file> <key>     one object; multipart above 64 MiB
//!   fb-agent objects get <key> <file>     to <file>.tmp, then renamed
//!   fb-agent objects sync <dir> <prefix>  each file of <dir> not listed under <prefix> yet
//!   fb-agent objects rm <key>             gone already is done
//!   fb-agent objects exists <key>         exit status 0 when it exists, 1 when not, 2 on an error
//!                                         (a missing bucket or container is an error)
//!
//! Keys and prefixes are relative to OBJECT_PREFIX. The store is OBJECT_STORE: s3, azure or gcs,
//! configured by the crate's own variables, which its builders read (AWS_*, AZURE_*, GOOGLE_*):
//! the bucket (AWS_BUCKET, AZURE_CONTAINER_NAME, GOOGLE_BUCKET), the credentials, or none for the
//! pod's identity (IRSA, EKS Pod Identity, the instance profile; AKS workload identity, managed
//! identity; GKE workload identity, Application Default Credentials).
//!
//! Besides: OBJECT_SSE_C_KEY_FILE, S3's customer key as the file `aws s3 --sse-c-key fileb://`
//! reads; AWS_SERVER_SIDE_ENCRYPTION takes the aws CLI's values (AES256, aws:kms, with
//! AWS_SSE_KMS_KEY_ID); an http:// AWS_ENDPOINT is allowed without AWS_ALLOW_HTTP. OBJECT_CA_FILE, PEM certificates trusted besides the
//! Mozilla roots built in (a private endpoint's CA): the system's store is not read, an instance
//! image need not have one.
//!
//! object_store is async: this subcommand alone runs a current-thread tokio runtime.

use crate::client::var;
use base64::Engine;
use futures::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path;
use object_store::{Certificate, ClientOptions, ObjectStore, ObjectStoreExt, PutPayload, WriteMultipart};
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

/// Above this a file is uploaded in parts
const MULTIPART_THRESHOLD: u64 = 64 << 20;
/// The size of a part (S3 allows 10,000 parts: 160 GiB)
const PART_SIZE: usize = 16 << 20;
/// Parts in flight at once
const PARTS_IN_FLIGHT: usize = 4;

/// What went wrong, on one line, and the exit status
struct Failed(String, u8);

impl From<object_store::Error> for Failed {
    fn from(error: object_store::Error) -> Self {
        Failed(one_line(&error.to_string()), 1)
    }
}

impl From<std::io::Error> for Failed {
    fn from(error: std::io::Error) -> Self {
        Failed(one_line(&error.to_string()), 1)
    }
}

fn one_line(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The object store path of a key under the prefix; empty parts (a leading, trailing or double
/// slash) are left out, as S3 keys built by `s3Uri` have none
fn path(prefix: &str, key: &str) -> Path {
    Path::from_iter(prefix.split('/').chain(key.split('/')).filter(|part| !part.is_empty()))
}

/// The client options: the Mozilla roots and OBJECT_CA_FILE only; no total timeout (a backup of
/// many gigabytes takes as long as it takes), a minute without a byte read is a failure
fn client_options() -> Result<ClientOptions, Failed> {
    let mut options = ClientOptions::new()
        .with_no_system_certificates(true)
        .with_timeout_disabled()
        .with_read_timeout(Duration::from_secs(60))
        .with_connect_timeout(Duration::from_secs(10));
    for root in webpki_root_certs::TLS_SERVER_ROOT_CERTS {
        options = options.with_root_certificate(Certificate::from_der(root.as_ref())?);
    }
    let ca = var("OBJECT_CA_FILE");
    if !ca.is_empty() {
        let pem = fs::read(&ca).map_err(|e| Failed(format!("cannot read OBJECT_CA_FILE {ca}: {e}"), 2))?;
        for certificate in Certificate::from_pem_bundle(&pem)? {
            options = options.with_root_certificate(certificate);
        }
    }
    Ok(options)
}

/// The store OBJECT_STORE names, from the environment
fn store() -> Result<Arc<dyn ObjectStore>, Failed> {
    let options = client_options()?;
    let kind = var("OBJECT_STORE");
    Ok(match kind.as_str() {
        "s3" => {
            let mut builder = AmazonS3Builder::from_env().with_client_options(options);
            if var("AWS_ENDPOINT").starts_with("http://") {
                builder = builder.with_allow_http(true);
            }
            let customer_key = var("OBJECT_SSE_C_KEY_FILE");
            if !customer_key.is_empty() {
                let key =
                    fs::read(&customer_key).map_err(|e| Failed(format!("cannot read OBJECT_SSE_C_KEY_FILE {customer_key}: {e}"), 2))?;
                if key.len() != 32 {
                    return Err(Failed(
                        format!("OBJECT_SSE_C_KEY_FILE {customer_key}: {} bytes, an SSE-C key has 32", key.len()),
                        2,
                    ));
                }
                let encoded = base64::engine::general_purpose::STANDARD.encode(key);
                builder = builder
                    .with_config("aws_server_side_encryption".parse()?, "sse-c")
                    .with_config("aws_sse_customer_key_base64".parse()?, encoded);
            }
            Arc::new(builder.build()?)
        }
        "azure" => Arc::new(MicrosoftAzureBuilder::from_env().with_client_options(options).build()?),
        "gcs" => Arc::new(GoogleCloudStorageBuilder::from_env().with_client_options(options).build()?),
        other => return Err(Failed(format!("OBJECT_STORE: s3, azure or gcs, not \"{other}\""), 2)),
    })
}

/// The names of the objects directly under the prefix
async fn list(store: &dyn ObjectStore, prefix: &Path) -> Result<Vec<String>, Failed> {
    let listed = store.list_with_delimiter(Some(prefix)).await?;
    Ok(listed
        .objects
        .iter()
        .filter_map(|meta| meta.location.filename().map(str::to_string))
        .collect())
}

async fn put(store: &dyn ObjectStore, file: &str, key: &Path) -> Result<(), Failed> {
    let mut source = File::open(file).map_err(|e| Failed(format!("cannot open {file}: {e}"), 1))?;
    let size = source.metadata()?.len();
    if size <= MULTIPART_THRESHOLD {
        let mut content = Vec::with_capacity(size as usize);
        source.read_to_end(&mut content)?;
        store.put(key, PutPayload::from(content)).await?;
        return Ok(());
    }
    let mut upload = WriteMultipart::new_with_chunk_size(store.put_multipart(key).await?, PART_SIZE);
    let mut buffer = vec![0u8; 1 << 20];
    loop {
        let read = match source.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => read,
            Err(error) => {
                let _ = upload.abort().await;
                return Err(error.into());
            }
        };
        if let Err(error) = upload.wait_for_capacity(PARTS_IN_FLIGHT).await {
            let _ = upload.abort().await;
            return Err(error.into());
        }
        upload.write(&buffer[..read]);
    }
    upload.finish().await?;
    Ok(())
}

async fn get(store: &dyn ObjectStore, key: &Path, file: &str) -> Result<(), Failed> {
    let partial = format!("{file}.tmp");
    let fetched = async {
        let mut stream = store.get(key).await?.into_stream();
        let mut target = File::create(&partial).map_err(|e| Failed(format!("cannot write {partial}: {e}"), 1))?;
        while let Some(chunk) = stream.next().await {
            target.write_all(&chunk?)?;
        }
        target.sync_all()?;
        Ok::<_, Failed>(())
    }
    .await;
    match fetched {
        Ok(()) => Ok(fs::rename(&partial, file)?),
        Err(failed) => {
            let _ = fs::remove_file(&partial);
            Err(failed)
        }
    }
}

/// Uploads the regular files of the directory that are not under the prefix yet, in name order;
/// the names uploaded on stdout
async fn sync(store: &dyn ObjectStore, dir: &str, prefix: &Path, prefix_text: &str) -> Result<(), Failed> {
    let present: BTreeSet<String> = list(store, prefix).await?.into_iter().collect();
    let mut names: Vec<String> = fs::read_dir(dir)
        .map_err(|e| Failed(format!("cannot read {dir}: {e}"), 1))?
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_file()))
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| !present.contains(name))
        .collect();
    names.sort();
    let base = var("OBJECT_PREFIX");
    for name in names {
        let key = path(&base, &format!("{prefix_text}/{name}"));
        put(store, &format!("{}/{name}", dir.trim_end_matches('/')), &key).await?;
        println!("{name}");
    }
    Ok(())
}

async fn run(arguments: &[&str]) -> Result<(), Failed> {
    let base = var("OBJECT_PREFIX");
    match arguments {
        ["list", prefix] => {
            let store = store()?;
            let mut out = std::io::stdout().lock();
            for name in list(store.as_ref(), &path(&base, prefix)).await? {
                writeln!(out, "{name}")?;
            }
            Ok(())
        }
        ["put", file, key] => put(store()?.as_ref(), file, &path(&base, key)).await,
        ["get", key, file] => get(store()?.as_ref(), &path(&base, key), file).await,
        ["sync", dir, prefix] => sync(store()?.as_ref(), dir, &path(&base, prefix), prefix).await,
        ["rm", key] => match store()?.delete(&path(&base, key)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(error.into()),
        },
        ["exists", key] => {
            let store = store()?;
            let key = path(&base, key);
            match store.head(&key).await {
                Ok(_) => Ok(()),
                // a HEAD's 404 has no body: the object, or its bucket? The listing of its parent
                // fails when the bucket (container) is missing
                Err(object_store::Error::NotFound { .. }) => {
                    let parent = Path::from_iter(
                        key.parts()
                            .collect::<Vec<_>>()
                            .split_last()
                            .map(|(_, rest)| rest.to_vec())
                            .unwrap_or_default(),
                    );
                    match store.list_with_delimiter(Some(&parent)).await {
                        Ok(_) => Err(Failed(String::new(), 1)),
                        Err(error) => Err(Failed(one_line(&error.to_string()), 2)),
                    }
                }
                Err(error) => Err(Failed(one_line(&error.to_string()), 2)),
            }
        }
        _ => Err(Failed(
            "usage: fb-agent objects list <prefix> | put <file> <key> | get <key> <file> | sync <dir> <prefix> | rm <key> | exists <key>"
                .into(),
            2,
        )),
    }
}

pub fn command(arguments: &[&str]) -> ExitCode {
    // rustls's provider for reqwest (built without one): ring, as the agent's own TLS
    let _ = rustls::crypto::ring::default_provider().install_default();
    let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("cannot start the runtime: {error}");
            return ExitCode::from(2);
        }
    };
    match runtime.block_on(run(arguments)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(Failed(message, code)) => {
            if !message.is_empty() {
                eprintln!("{message}");
            }
            ExitCode::from(code)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_under_the_prefix() {
        assert_eq!(path("", "backup.fbk").as_ref(), "backup.fbk");
        assert_eq!(path("prod", "backup.fbk").as_ref(), "prod/backup.fbk");
        assert_eq!(path("/prod/x/", "journals/").as_ref(), "prod/x/journals");
        assert_eq!(
            path("prod", "journals/ledger.fdb/ledger.fdb.journal-000000001").as_ref(),
            "prod/journals/ledger.fdb/ledger.fdb.journal-000000001"
        );
        assert_eq!(path("a//b", "/c").as_ref(), "a/b/c");
    }

    #[test]
    fn the_mozilla_roots_are_built_in() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        assert!(webpki_root_certs::TLS_SERVER_ROOT_CERTS.len() > 100);
        assert!(client_options().is_ok());
    }

    #[test]
    fn an_unknown_store_and_bad_usage_are_refused() {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let Err(Failed(message, code)) = runtime.block_on(run(&["copy", "a", "b"])) else {
            panic!("accepted")
        };
        assert_eq!(code, 2);
        assert!(message.starts_with("usage: fb-agent objects"));
    }
}
