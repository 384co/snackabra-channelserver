export const VERSION = "2.0.0 (pre) (build 33)"
export const DEBUG = true;
export const DEBUG2 = false;

// this section has some type definitions that helps us with CF types
export type EnvType = {
    channels: DurableObjectNamespace, // 'main': ChannelServerAPI
    MESSAGES_NAMESPACE: KVNamespace, // global messages namespace
    LEDGER_NAMESPACE: KVNamespace, // used for storage tokens and storage approvals; also accessed by storage server
    KEYS_NAMESPACE: KVNamespace,
    STORAGE_SERVER: string,
    DEBUG_ON: boolean,
    VERBOSE_ON: boolean,
    notifications: Fetcher,
    WEB_NOTIFICATION_SERVER: string,
    ENVIRONMENT?: string,
  }
