import type {
  CaptureHandle,
  CaptureSource,
  InitializeOptions,
  SourceLifecycle,
  SourceSink,
} from 'semantic-layer-capture';
import type { CloudUploader, CloudUploaderOptions } from 'semantic-layer-cloud';

export type { CaptureSource, SourceLifecycle, SourceSink };

export type CaptureHandleLike = Pick<
  CaptureHandle,
  'installSource' | 'status' | 'flush' | 'shutdown'
>;

export type CloudUploaderLike = Pick<
  CloudUploader,
  'enqueueArtifact' | 'flush' | 'status' | 'shutdown'
>;

export type PluginDependencies = {
  createRunCapture(options: InitializeOptions): CaptureHandleLike;
  createUploader(options: CloudUploaderOptions): CloudUploaderLike;
};

export type ResolvedPluginConfig = {
  endpoint: string;
  ingestKey: string;
  identityKey: string;
  installationId: string;
  serviceName: string;
  outputDirectory?: string;
  spoolDirectory?: string;
  maxSpoolBytes?: number;
};
