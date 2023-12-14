export const VERSION = "2.0.0 (pre) (build 06)"
export const DEBUG = true;
export const DEBUG2 = false;

// this section has some type definitions that helps us with CF types
export type EnvType = {
    // ChannelServerAPI
    channels: DurableObjectNamespace,
    // used for worker-to-worker (see toml)
    notifications: Fetcher
    // primarily for raw uploads and raw budget allocations
    SERVER_SECRET: string,
    // KV Namespaces
    MESSAGES_NAMESPACE: KVNamespace,
    KEYS_NAMESPACE: KVNamespace,
    LEDGER_NAMESPACE: KVNamespace,
    IMAGES_NAMESPACE: KVNamespace,
    RECOVERY_NAMESPACE: KVNamespace,
    // looks like: '{"key_ops":["encrypt"],"ext":true,"kty":"RSA","n":"6WeMtsPoblahblahU3rmDUgsc","e":"AQAB","alg":"RSA-OAEP-256"}'
    LEDGER_KEY: string,
    STORAGE_SERVER: string,
    ENVIRONMENT?: string,
  }
  