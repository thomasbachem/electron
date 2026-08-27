// Modules available to Node.js worker threads, please sort alphabetically.
export const workerThreadModuleList: ElectronInternal.ModuleEntry[] = [
  { name: 'protocol', loader: () => require('./protocol') }
];
