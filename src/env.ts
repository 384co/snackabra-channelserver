// this section has some type definitions that helps us with CF types
export type EnvType = {
  VERSION: string,
  channels: DurableObjectNamespace, // 'main': ChannelServerAPI
  MESSAGES_NAMESPACE: KVNamespace, // global messages namespace
  LEDGER_NAMESPACE: KVNamespace, // used for storage tokens and storage approvals; also accessed by storage server
  PAGES_NAMESPACE: KVNamespace, // used for Pages, also accessible by storage server
  KEYS_NAMESPACE: KVNamespace,
  STORAGE_SERVER: string,
  DEBUG_ON: boolean,
  VERBOSE_ON: boolean,
  notifications: Fetcher,
  WEB_NOTIFICATION_SERVER: string,
  ENVIRONMENT?: string,
}
