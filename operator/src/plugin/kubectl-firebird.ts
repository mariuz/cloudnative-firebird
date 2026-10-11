#!/usr/bin/env node
/**
 * kubectl-firebird, the kubectl plugin's executable (see cli.ts). A dependency of the Kubernetes
 * client warns about a deprecated Node module when it loads: silenced before it is loaded.
 */
process.noDeprecation = true;
// eslint-disable-next-line @typescript-eslint/no-require-imports
void (require('./main') as typeof import('./main')).main();
