// this section has some type definitions that helps us with CF types
export type EnvType = {
  VERSION: string,
  channels: DurableObjectNamespace, // 'main': ChannelServerAPI
  MESSAGES_NAMESPACE: KVNamespace, // global messages namespace
  LEDGER_NAMESPACE: KVNamespace, // used for storage tokens and storage approvals; also accessed by storage server
  PAGES_NAMESPACE: KVNamespace, // used for Pages, also accessible by storage server
  KEYS_NAMESPACE: KVNamespace,
  STORAGE_SERVER_NAME: string,
  DEBUG_LEVEL_1: boolean,
  VERBOSE_ON: boolean,
  LOG_ERRORS: boolean,
  notifications: Fetcher,
  WEB_NOTIFICATION_SERVER: string,
  ENVIRONMENT?: string,
  STORAGE_SERVER_BINDING: any,
  IS_LOCAL: boolean, // set to true by 'yarn start' to signal we're on local dev
}
