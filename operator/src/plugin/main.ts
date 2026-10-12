import { spawn } from 'child_process';
import { mkdir, writeFile } from 'fs/promises';
import {
  AppsV1Api,
  CoreV1Api,
  CustomObjectsApi,
  KubeConfig,
  PatchStrategy,
  setHeaderOptions,
} from '@kubernetes/client-node';
import { API_GROUP, API_VERSION, FirebirdBackup, FirebirdCluster, FirebirdDatabase, FirebirdScheduledBackup } from '../types';
import { PluginApi, run } from './cli';

/**
 * kubectl-firebird: on the PATH, `kubectl firebird ...` runs it (kubectl plugins). It uses the
 * kubeconfig kubectl uses (KUBECONFIG, ~/.kube/config) and its current context, or --context.
 */

const MERGE_PATCH = setHeaderOptions('Content-Type', PatchStrategy.MergePatch);

function contextFlag(argv: string[]): string | undefined {
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--') return undefined;
    if (argv[i] === '--context') return argv[i + 1];
    if (argv[i].startsWith('--context=')) return argv[i].slice('--context='.length);
  }
  return undefined;
}

function kubeApi(kubeConfig: KubeConfig): PluginApi {
  const custom = kubeConfig.makeApiClient(CustomObjectsApi);
  const core = kubeConfig.makeApiClient(CoreV1Api);
  const apps = kubeConfig.makeApiClient(AppsV1Api);
  const crd = (namespace: string, plural: string) => ({ group: API_GROUP, version: API_VERSION, namespace, plural });
  return {
    getCluster: async (namespace, name) =>
      (await custom.getNamespacedCustomObject({ ...crd(namespace, 'firebirdclusters'), name })) as FirebirdCluster,
    patchCluster: async (namespace, name, body) => {
      await custom.patchNamespacedCustomObject({ ...crd(namespace, 'firebirdclusters'), name, body }, MERGE_PATCH);
    },
    listPods: async (namespace, labelSelector) => (await core.listNamespacedPod({ namespace, labelSelector })).items,
    patchPod: async (namespace, name, body) => {
      await core.patchNamespacedPod({ namespace, name, body }, MERGE_PATCH);
    },
    deletePod: async (namespace, name) => {
      await core.deleteNamespacedPod({ namespace, name });
    },
    patchStatefulSet: async (namespace, name, body) => {
      await apps.patchNamespacedStatefulSet({ namespace, name, body }, MERGE_PATCH);
    },
    listBackups: async (namespace) =>
      ((await custom.listNamespacedCustomObject(crd(namespace, 'firebirdbackups'))) as { items?: FirebirdBackup[] }).items ?? [],
    listScheduledBackups: async (namespace) =>
      ((await custom.listNamespacedCustomObject(crd(namespace, 'firebirdscheduledbackups'))) as { items?: FirebirdScheduledBackup[] })
        .items ?? [],
    listDatabases: async (namespace) =>
      ((await custom.listNamespacedCustomObject(crd(namespace, 'firebirddatabases'))) as { items?: FirebirdDatabase[] }).items ?? [],
    createBackup: async (namespace, body) => {
      await custom.createNamespacedCustomObject({ ...crd(namespace, 'firebirdbackups'), body });
    },
    getStatefulSet: (namespace, name) => apps.readNamespacedStatefulSet({ namespace, name }),
    listServices: async (namespace, labelSelector) => (await core.listNamespacedService({ namespace, labelSelector })).items,
    listClaims: async (namespace, labelSelector) => (await core.listNamespacedPersistentVolumeClaim({ namespace, labelSelector })).items,
    listEvents: async (namespace) => (await core.listNamespacedEvent({ namespace })).items,
    readPodLog: async (namespace, name, container) => String((await core.readNamespacedPodLog({ namespace, name, container })) ?? ''),
  };
}

export async function main(): Promise<void> {
  const argv = process.argv.slice(2);
  const kubeConfig = new KubeConfig();
  kubeConfig.loadFromDefault();
  const context = contextFlag(argv);
  if (context) kubeConfig.setCurrentContext(context);
  const namespace = kubeConfig.getContextObject(kubeConfig.getCurrentContext())?.namespace ?? 'default';
  const code = await run(
    argv,
    kubeApi(kubeConfig),
    {
      out: (text) => process.stdout.write(`${text}\n`),
      err: (text) => process.stderr.write(`${text}\n`),
      kubectl: (args) =>
        new Promise((resolve) => {
          const child = spawn(process.env.KUBECTL ?? 'kubectl', args, { stdio: 'inherit' });
          child.on('error', (err) => {
            process.stderr.write(`error: could not run kubectl: ${err.message}\n`);
            resolve(1);
          });
          child.on('exit', (status) => resolve(status ?? 1));
        }),
      interactive: Boolean(process.stdin.isTTY),
      now: () => new Date(),
      makeDir: async (path) => {
        await mkdir(path);
      },
      writeFile: (path, content) => writeFile(path, content),
    },
    namespace,
  );
  process.exitCode = code;
}

