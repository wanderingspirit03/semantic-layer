import { createCloudUploader } from 'semantic-layer-cloud';
import { createCapture } from 'semantic-layer-capture';
import { createPluginDefinition } from './plugin.js';

const plugin = createPluginDefinition({
  createRunCapture: createCapture,
  createUploader: createCloudUploader,
});

export default plugin;
export { createPluginDefinition, pseudonymizeSession, resolveConfig } from './plugin.js';
export type { PluginDependencies, ResolvedPluginConfig } from './contracts.js';
