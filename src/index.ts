/// <reference types="@cloudflare/workers-types" />

/*
   Copyright (C) 2022-2023 384, Inc., All Rights Reserved
   Copyright (C) 2019-2022 Magnusson Institute, All Rights Reserved

   "Snackabra" is a registered trademark
   "384" is a registered trademark

   This program is free software: you can redistribute it and/or
   modify it under the terms of the GNU Affero General Public License
   as published by the Free Software Foundation, either version 3 of
   the License, or (at your option) any later version.

   This program is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public
   License along with this program.  If not, see www.gnu.org/licenses/

*/

// this feature is under development
const MESSAGE_HISTORY = false;

import {
  Channel,
  sbCrypto, extractPayload, assemblePayload, ChannelMessage,
  stripChannelMessage, setDebugLevel, SBStorageToken,
  validate_SBStorageToken, SBStorageTokenPrefix,
  SB384, arrayBufferToBase62, jsonParseWrapper,
  version, validate_ChannelMessage, validate_SBChannelData,
  SBUserPublicKey,
  SBError,
  Snackabra, // stringify_SBObjectHandle,
  // arrayBufferToBase64url,
  MessageHistoryEntry, MessageHistoryDirectory,
} from 'snackabra';

import type { SBChannelId, ChannelAdminData, SBUserId, SBChannelData, ChannelApiBody, SBObjectHandle } from 'snackabra';

import type { EnvType } from './env'

import {
  _sb_assert, returnResult, returnResultJson, returnError, returnSuccess, serverConstants, serverApiCosts, _appendBuffer,
  processApiBody,
  getServerStorageToken, ANONYMOUS_CANNOT_CONNECT_MSG,
  genKey, genKeyPrefix,
  return304,
  textLikeMimeTypes,
  dbg,
  serverFetch,
} from './workers'

function arrayBufferToText(buf: ArrayBuffer) {
  const decoder = new TextDecoder('utf-8'); // Create a new TextDecoder instance for UTF-8 encoded text
  return decoder.decode(new Uint8Array(buf)); // Decode an ArrayBuffer to text
}

const SEPx = '='.repeat(76)
const SEP = '\n' + SEPx + '\n'
const SEPxStar = '\n' + '*'.repeat(76) + '\n'

const DBG0 = true; // set to true to enable specific debug output

// called on all 'entry points' to set the debug level
function setServerDebugLevel(env: EnvType) {
  dbg.DEBUG = env.DEBUG_ON ? true : false;
  dbg.LOG_ERRORS = env.LOG_ERRORS || dbg.DEBUG ? true : false;
  dbg.DEBUG2 = env.VERBOSE_ON ? true : false;
  if (dbg.DEBUG2) setDebugLevel(dbg.DEBUG) // poke jslib
}

const base62mi = "0123456789ADMRTxQjrEywcLBdHpNufk" // "v05.05" (strongpinVersion ^0.6.0)
const base62Regex = new RegExp(`[${base62mi}]`);

export { default } from './workers'

const CHANNEL_STORAGE_MULTIPLIER_PERMA = serverApiCosts.CHANNEL_STORAGE_MULTIPLIER
const CHANNEL_STORAGE_MULTIPLIER_TEMP = serverApiCosts.CHANNEL_STORAGE_MULTIPLIER_TTL_ZERO

// todo (below): most things go through DO only, but there are probably more API
// endpoints we can handle before callDurableObject(), at which point we go from
// stateless (scalable) servlets to a singleton.

// // simple way to track what our origin is
// var myOrigin: string = '';


export async function handleApiRequest(path: Array<string>, request: Request, env: EnvType) {
  setServerDebugLevel(env)

  // if (!myOrigin) myOrigin = new URL(request.url).origin;

  try {
    switch (path[0]) {
      case 'info':
        return returnResultJson(request, serverInfo(request, env))
      case 'channel':
        if (!path[1]) return returnError(request, "ERROR: invalid API (should be '/api/v2/channel/<channelId>/<api>')", 400);
        return callDurableObject(path[1], path.slice(2), request, env);
      case 'page':
          // todo: if the pages view is 'locked', we need to proceed to the DO,
          // but we need to construct how to call the DO (eg, we need to know
          // the channel id from the api payload etc)
          if (dbg.DEBUG) console.log("==== Page Fetch ====")
          // make sure there is a page key
          if (!path[1]) return returnError(request, "ERROR: invalid API (should be '/api/v2/page/<pageKey>')", 400);
          const pageKey = path[1]
          // some sanity checks: pageKey must be regex b32, and at least 6 characters long, and no more than 48 characters long
          if (!base62Regex.test(pageKey) || pageKey.length < 6 || pageKey.length > 48) {
            if (dbg.LOG_ERRORS) console.error("ERROR: invalid page key [L085]")
            return returnError(request, ANONYMOUS_CANNOT_CONNECT_MSG, 401);
          }
          const pageKeyKVprefix = genKeyPrefix(pageKey, 'G')
          // we now use 'list' to get all prefix matching this
          const listOptions: DurableObjectListOptions = { limit: 4, prefix: pageKeyKVprefix, reverse: true };
          const resultList = await env.PAGES_NAMESPACE.list(listOptions)
          if (resultList.keys.length === 0) {
            // if there are no entries, well, there's no such Page
            if (dbg.LOG_ERRORS) console.error(`ERROR: no entry found for page key ${pageKey} [L093]`)
            return returnError(request, ANONYMOUS_CANNOT_CONNECT_MSG, 401);
          }
          if (resultList.keys.length !== 1) {
            // if there is an entry, then there must only be one
            if (dbg.LOG_ERRORS) console.error("ERROR: found multiple entries, should not happen [L095]", resultList)
            return returnError(request, ANONYMOUS_CANNOT_CONNECT_MSG, 401);
          }
          // now we grab it's full key and fetch the object
          const pageKeyKV = resultList.keys[0]!.name
          // now read from the PAGES namespace
          const { value, metadata } = await env.PAGES_NAMESPACE.getWithMetadata(pageKeyKV, { type: "arrayBuffer" });
          const pageMetaData = metadata as PageMetaData
          if (dbg.DEBUG) console.log("Got this SPECIFIC entry from KV: ", value, metadata)
          if (!value) {
            if (dbg.LOG_ERRORS) console.error("ERROR: no value found for page key [L105]")
            return returnError(request, ANONYMOUS_CANNOT_CONNECT_MSG, 401);
          }
          // some meta data sanity checks
          // unless otherwise requested, we default to shortest permitted
          const shortestPrefix = pageMetaData.shortestPrefix || 6
          if (pageKey.length < shortestPrefix) {
            if (dbg.LOG_ERRORS) console.error("ERROR: page key too short [L112]")
            return returnError(request, ANONYMOUS_CANNOT_CONNECT_MSG, 401);
          }
          // todo: improve cache headers, including stale-while-revalidate cache policy
          // for now at least we handle Etags
          const clientEtag = request.headers.get("If-None-Match");
          if (clientEtag === pageMetaData.hash) {
            if (dbg.DEBUG) console.log("Returning 304 (not modified)")
            return return304(request, clientEtag); // mirror it back, it hasn't changed
          }
          let returnValue: ArrayBuffer | string = value
          if (pageMetaData.type && textLikeMimeTypes.has(pageMetaData.type)) {
            if (dbg.DEBUG) console.log("It was stored explicit text-like type, recoding to text")
            returnValue = arrayBufferToText(value)
          } else {
            if (dbg.DEBUG) console.log("Not recoding return result.")
          }
          return returnResult(request, returnValue, { type: pageMetaData.type, headers: { "Etag": pageMetaData.hash }})
      case "notifications":
        return returnError(request, "Device (Apple) notifications are disabled (use web notifications)", 400);
      case "getLastMessageTimes":
        // ToDo: this needs to be modified to receive a userId for each channel requested
        //       as well as limit how many can be queried at once
        return returnError(request, "getLastMessageTimes disabled on this server (see release notes)", 400)
      default:
        return returnError(request, "Not found (this is an API endpoint, the URI was malformed)", 404)
    }
  } catch (error: any) {
    return returnError(request, `[API Call error] [${request.url}]: \n` + error.message + '\n' + error.stack, 500);
  }
}

// calling this switches from 'generic' (anonymous) microservice to a
// (synchronous) Durable Object (unique per channel)
async function callDurableObject(channelId: SBChannelId, path: Array<string>, request: Request, env: EnvType) {
  const durableObjectId = env.channels.idFromName(channelId);
  // locate to west north america (not relocated afterwards); todo: see if
  // there's a simple way to select 'enam' if that's closer
  const durableObject = env.channels.get(durableObjectId, { locationHint: 'wnam'});
  const newUrl = new URL(request.url);
  newUrl.pathname = "/" + channelId + "/" + path.join("/");
  const newRequest = new Request(newUrl.toString(), request);
  if (dbg.DEBUG) {
    console.log(
      "==== callDurableObject():\n",
      "channelId:", channelId, "\n",
      "path:", path, '\n',
      durableObjectId, '\n')
    // "newUrl:\n", newUrl, SEP) // very wordy
    if (dbg.DEBUG2) { console.log(request); console.log(env) }
  }
  // we direct the fetch 'at' the durable object
  return durableObject.fetch(newRequest);
}

// returns information on the channel server
// notably the storage server URL that should be used for this channel (from wrangler.toml)
function serverInfo(request: Request, env: EnvType) {
  const url = new URL(request.url);
  let storageUrl: string | null = null
  if (url.hostname === 'localhost' && url.port === '3845') {
    storageUrl = 'http://localhost:3843';
  } else if (url.protocol === 'https:' && url.hostname.split('.').length >= 2) {
    const storageServer = env.STORAGE_SERVER_NAME; // Replace with your environment variable
    const domainParts = url.hostname.split('.');
    if (storageServer && domainParts.length >= 2) {
      domainParts[0] = storageServer; // Replace the top-level domain with STORAGE_SERVER_NAME
      storageUrl = `https://${domainParts.join('.')}`;
    }
  } // and if nothing matches then storageUrl is null:
  if (!storageUrl) {
    const msg = "ERROR: Could not determine storage server URL"
    console.error(msg)
    return { success: false, error: msg }
  }
  var retVal = {
    version: env.VERSION,
    storageServer: storageUrl,
    jslibVersion: version,

    // ... we would have to go to DO for this info, and we don't want to
    // apiEndpoints: {
    //   visitor: loadedVisitorApiEndpoints,
    //   owner: loadedOwnerApiEndpoints
    // },

  }
  return retVal
}

type ApiCallMap = {
  [key: string]: ((request: Request, apiBody: ChannelApiBody) => Promise<Response>) | undefined;
};

interface SessionInfoHibernated {
  // the following are included in hibernation
  userId: SBUserId,
  channelId: SBChannelId,
  isOwner: boolean,
}

interface SessionInfo extends SessionInfoHibernated {
  // the following are re-created on return from hibernation
  // (non-serializable or trivially re-creatable)
  userKeys: SB384,
  webSocket: WebSocket,
  ready: boolean,
  quit: boolean,
}

// interface HibernationInfo {
//   userId: SBUserId,
//   channelId: SBChannelId,
//   isOwner: boolean,
//   // apiBody: ChannelApiBody,
//   // channelData: SBChannelData,
// }

// var loadedVisitorApiEndpoints: Array<string> = []
// var loadedOwnerApiEndpoints: Array<string> = []

interface PageMetaData {
  id: string,
  owner: SBUserId,
  size: number,
  lastModified: number,
  shortestPrefix: number,
  hash: string, // base62 string of sha256 hash of contents
  type: string, // MIME type, if present then pre-process and 'type' the response
}

let storageFetchFunction: ((input: RequestInfo | URL, init?: RequestInit) => Promise<Response>) | null = null;
let storageServerBinding: any = null;


// in a comment section, list the most common MIME types on the Internet
// https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types/Common_types

interface TTL0Buffer {
  first: number, // points to first entry (unless equal to 'last', in which case buffer is empty)
  last: number, // always points to next 'free' spot
  totalSize: number,
   // from ring buffer, to message Map; 'x' is expiration, 'id' is message key, 't' is userId (if targeted)
  ring: Map<number, { x: number, id: string, t: SBUserId | undefined }>
  messages: Map<string, ArrayBuffer>
}

const TTL0_MAX_MESSAGES = 1024; // have a modulo index ring buffer
const TTL0_MAX_SPACE = 2 * 1024 * 1024; // 2 MiB; note this is in-memory only
const TTL0_EXPIRATION = 30 * 1000; // 30 seconds

/**
 * Ring buffer for all recent messages; includes TTL0 messages but also everything else.
 * This is dumped upon hibernation.
 */
export class TTL0BufferClass {
  constructor(public buffer: TTL0Buffer =
    { first: 0, last: 0, totalSize: 0, ring: new Map(), messages: new Map() })
    {
      // we do a few sanity checks on the buffer before using it
      let tail = this.buffer.first;
      while (tail !== this.buffer.last) {
        const id = this.buffer.ring.get(tail)?.id;
        _sb_assert(id, "Message not found in buffer (Internal Error) [L280]");
        const m = this.buffer.messages.get(id!);
        _sb_assert(m, "Message not found in buffer (Internal Error) [L282]");
        tail = (tail + 1) % TTL0_MAX_MESSAGES;
      }
      let totalSize = 0;
      for (const m of this.buffer.messages.values()) totalSize += m.byteLength;
      _sb_assert(totalSize === this.buffer.totalSize, "Total size does not match (Internal Error) [L290]");
      this.deleteExpired(); // looks good, let's clean it up
      if (DBG0 && this.buffer.first !== this.buffer.last) console.log("Loaded TTL0BufferClass from storage: ", this.buffer);
    }

  // remove from the 'top' (start) of the buffer
  removeFirst() {
    if (this.buffer.first === this.buffer.last) return; // nothing to remove
    const id = this.buffer.ring.get(this.buffer.first)?.id;
    _sb_assert(id, "Message not found in buffer (Internal Error) [L301]");
    this.buffer.ring.delete(this.buffer.first);
    const m = this.buffer.messages.get(id!);
    _sb_assert(m, "Message not found in buffer (Internal Error) [L304]");
    this.buffer.totalSize -= m!.byteLength;
    this.buffer.messages.delete(id!);
    this.buffer.first = (this.buffer.first + 1) % TTL0_MAX_MESSAGES;
  }

  // note: when we add a message, we do not need to trim for SIZE until AFTER it's been added
  addMessage(id: string, m: ArrayBuffer, userId?: SBUserId) {
    _sb_assert(!this.buffer.messages.has(id), "Message already in buffer (Internal Error) [L312]");
    if (dbg.DEBUG2) console.log("... adding message to buffer:", id)
    this.deleteExpired(); // just keeping things tight
    this.buffer.totalSize += m.byteLength;
    this.buffer.ring.set(this.buffer.last, { x: Date.now() + TTL0_EXPIRATION, id: id, t: userId});
    this.buffer.messages.set(id, m);
    this.buffer.last = (this.buffer.last + 1) % TTL0_MAX_MESSAGES;
    if (this.buffer.last === this.buffer.first)
      // if we're hitting our tail, we delete the tail; 'last' must point to a free spot
      this.removeFirst();
    while (this.buffer.totalSize > TTL0_MAX_SPACE) {
      // if necessary, guaranteed to be successful
      this.removeFirst();
    }
  }
    
  // remove all messages that have expire
  deleteExpired() {
    const now = Date.now();
    while (this.buffer.ring.get(this.buffer.first)?.x! < now) {
      if (DBG0) console.log("... deleting expired message from buffer")
      this.removeFirst();
    }
  }

  get messages() {
    this.deleteExpired();
    return this.buffer.messages;
  }

  // getMessages(prefix?: string) {
  //   this.deleteExpired();
  //   if (!prefix) return this.buffer.messages;
  //   // otherwise we filter by matching beginning of id
  //   const filtered = new Map<string, ArrayBuffer>(
  //     [...this.buffer.messages].filter(([id]) => id.startsWith(prefix))
  //   );
  //   return filtered;
  // }

  // returns any message keys in the buffer that matches prefix
  getMessageKeys(prefix?: string): Set<string> {
    this.deleteExpired();
    if (!prefix)
      // return this.buffer.messages.keys();
      return new Set<string>(this.buffer.messages.keys());
    // otherwise we filter by matching beginning of id
    const filtered = new Set<string>(
      [...this.buffer.messages.keys()].filter(id => id.startsWith(prefix))
    );
    return filtered;
  }

  // returns (as a Map) any ttl0 messages if they match the keys
  getMessages(requestedKeys: Array<string>) {
    this.deleteExpired();
    const filtered = new Map<string, ArrayBuffer>();
    for (const key of requestedKeys) {
      const m = this.buffer.messages.get(key);
      if (m) filtered.set(key, m);
    }
    return filtered;
  }

  /** call the callback function for all messages that have a key that is
      greater than the given value; the callback function should return 'true'
      to stop the iteration, and 'false' to continue. The callback function is
      called with the message key and the message itself. Note that the
      iteration is in 'ring' order (modulo the buffer size etc). */
  iterate(callback: (key: string, userId: SBUserId | undefined, message: ArrayBuffer) => boolean, start: string) {
    // console.log("Asked for everything AFTER timestamp: ", start)
    this.deleteExpired();
    let tail = this.buffer.first;
    while (tail !== this.buffer.last) {
      const info = this.buffer.ring.get(tail)!
      _sb_assert(info, "Message not found in buffer (Internal Error) [L410]");
      const id = info.id
      _sb_assert(id, "Message not found in buffer (Internal Error) [L412]");
      const m = this.buffer.messages.get(id!);
      _sb_assert(m, "Message not found in buffer (Internal Error) [L414]");
      if (id! > start && callback(id!, info.t,  m!)) return;
      tail = (tail + 1) % TTL0_MAX_MESSAGES;
    }
  }

  get bufferOldestTimestamp() {
    this.deleteExpired();
    if (this.buffer.first === this.buffer.last) return '3'.repeat(26); // max timestamp (buffer empty)
    return Channel.deComposeMessageKey(this.buffer.ring.get(this.buffer.first)!.id).timestamp;
  }

  get getBuffer() {
    this.deleteExpired();
    return this.buffer;
  }
}




/**
 *
 * ChannelServer Durable Object Class
 *
 * One instance per channel
 */
export class ChannelServer implements DurableObject {
  channelId?: SBChannelId; // convenience copy of what's in channeldata

  /* all these properties are backed in storage (both global and DO KV) with below keys */
  /* ----- these do not change after creation ----- */
  /* 'channelData'         */ channelData?: SBChannelData
  /* 'motherChannel'       */ motherChannel?: SBChannelId
  /* ----- these are updated dynamically      ----- */
  /* 'storageLimit'        */ storageLimit: number = 0;        // current storage budget
  /* 'capacity'            */ capacity: number = 20;           // max number of (accepted) visitors
  /* 'latestTimestamp'     */ latestTimestamp: number = 0;     // monotonically increasing timestamp
  /* ----- these track access permissions      ----- */
  /* 'locked'              */ locked: boolean = false;
  /* 'visitors'            */ visitors: Map<SBUserId, SBUserPublicKey> = new Map();
  /* 'accepted'            */ accepted: Set<SBUserId> = new Set();
  /* ----- these track message history         ----- */
  /* 'messageCount'        */ messageCount: number = 0;        // total non-ephemeral messages in DO KV
  /* 'allMessagesCount'    */ allMessageCount: number = 0;     // total number of all messages
  /* 'newMessageCount'     */ newMessageCount: number = 0;     // the subset of 'messageCount' that has not been shardified  
  /* 'messageHistory'      */ messageHistory: MessageHistoryDirectory | null = null;

  /* the rest are for run time and are not backed up as such to KVs  */
  visitorKeys: Map<SBUserId, SB384> = new Map();    // convenience caching of SB384 objects for any visitors
  sessions: Map<SBUserId, SessionInfo> = new Map(); // track open (websocket) sessions, keyed by userId
  storage: DurableObjectStorage; // this is the DO storage, not the global KV storage
  visitorCalls: ApiCallMap; // API endpoints visitors are allowed to use
  ownerCalls: ApiCallMap;   // API endpoints that require ownership

  ttl0Buffer: TTL0BufferClass = new TTL0BufferClass(); // in-memory buffer for TTL0 messages

  webNotificationServer: string;

  hibernationPromise: Promise<void> | null = null; // used to track hibernation (if present, we're recovering from it)

  // ToDo: this caching was essentially working; disabling for now to focus on
  // feature completeness and correctness of new 'stream' design. It makes sense
  // to re-introduce this later: we have particular semantics on messages that a
  // general-purpose KV can't know. In particular, perma-messages (non-TTL) are
  // by defined immutable after creation, including timestamp; timestamps are
  // unique and monotonically increasing, and we can use all this to our
  // advantage to cache message keys and their indexes. The rough idea is to
  // cache in DO memory the last 1000 keys, and the last 100 messages. Keys
  // beyond this should automatically flow into shardified format. Another
  // advantage with disabling this now and adding it later, is that we can then
  // do some performance testing to see how much difference it makes, both in
  // terms of cost and latency.

  // // used for caching message keys
  // private messageKeysCache: string[] = []; // L2 cache
  // messageKeysCacheMap: Map<string, number> = new Map(); // 'indexes' into messageKeysCache
  // private lastCacheTimestamp: number = 0; // mirrors latestTimestamp (should be the same)
  // private recentKeysCache: Set<string> = new Set(); // L1 cache
  // private lastL1CacheTimestamp: number = 0; // similar but tracks the 'set'

  // dbg.DEBUG helper function to produce a string explainer of the current state
  #describe(): string {
    let s = 'CHANNEL STATE:\n';
    s += `channelId: ${this.channelData?.channelId}\n`;
    s += `latestTimestamp: ${this.latestTimestamp}\n`;
    s += `channelData: ${this.channelData}\n`;
    s += `locked: ${this.locked}\n`;
    s += `capacity: ${this.capacity}\n`;
    s += `storageLimit: ${this.storageLimit}\n`;
    return s;
  }

  // ToDo: evaluate if and where we might need 'blockConcurrencyWhile()'
  constructor(public state: DurableObjectState, public env: EnvType) {
    setServerDebugLevel(env)

    if (dbg.DEBUG) console.log("++++ channel server code loaded ++++ dbg.DEBUG is enabled ++++")
    else console.log("++++ channel server code loaded (NO DEBUG) ++++")
    if (dbg.DEBUG2) console.log("++++ dbg.DEBUG2 (verbose) enabled ++++")

    // durObj storage has a different API than global KV, see:
    // https://developers.cloudflare.com/workers/runtime-apis/durable-objects/#transactional-storage-api
    this.storage = state.storage; // per-DO (eg per channel) storage
    this.webNotificationServer = env.WEB_NOTIFICATION_SERVER

    this.visitorCalls = {
      "/getChannelKeys": this.#getChannelKeys.bind(this),
      "/getHistory": this.#getHistory.bind(this),
      "/getLatestTimestamp": this.#getLatestTimestamp.bind(this),
      "/getMessageKeys": this.#getMessageKeys.bind(this),
      "/getMessages": this.#getMessages.bind(this),
      "/getPubKeys": this.#getPubKeys.bind(this),
      "/getStorageLimit": this.#getStorageLimit.bind(this), // ToDo: should be per-userid basis
      "/getStorageToken": this.#getStorageToken.bind(this),
      "/send": this.#send.bind(this),
      // upload/download are disabled, but we provide feedback for now
      "/downloadChannel": this.#downloadChannel.bind(this),
      "/uploadChannel": this.#uploadChannel.bind(this),
      // deprecated/notfunctional
      "/registerDevice": this.#registerDevice.bind(this),
    }
    this.ownerCalls = {
      "/acceptVisitor": this.#acceptVisitor.bind(this),
      "/budd": this.#deposit.bind(this),
      "/getAdminData": this.#getAdminData.bind(this),
      "/lockChannel": this.#lockChannel.bind(this),
      "/setCapacity": this.#setCapacity.bind(this),
      "/setPage": this.#setPage.bind(this),
    }
    // loadedVisitorApiEndpoints = Object.keys(this.visitorCalls)
    // loadedOwnerApiEndpoints = Object.keys(this.ownerCalls)

    // todo: presumed to be a good idea to limit this?
    this.state.setHibernatableWebSocketEventTimeout(1 * 60 * 1000) // milliseconds thus this is 1 minute
    console.log("++++ setting websocket handler timeout to (seconds):", this.state.getHibernatableWebSocketEventTimeout()! / 1000)

    console.log("++++ setting autoresponse to timestamp ++++")
    this.state.setWebSocketAutoResponse(new WebSocketRequestResponsePair('ping', Channel.timestampToBase4String(this.latestTimestamp)))

    console.log("++++ CURRENT WEBSOCKETS  ++++")
    const wsList = this.state.getWebSockets()
    if (wsList.length > 0) {
      this.hibernationPromise = new Promise<void>(async (resolve, _reject) => {
        // ToDo: refactor, '#setUpNewSession' code overlap
        // we've returned from hibernation ... first we need to initialize
        if (DBG0) console.log(SEP, "[RETURNING FROM HIBERNATION] Beginning recovery ... ", SEP)
        const channelData = jsonParseWrapper(await (this.storage.get('channelData') as Promise<string>), 'L512') as SBChannelData
        await this.#initialize(channelData)
        if (DBG0) console.log(SEP, "[RETURNING FROM HIBERNATION] MAIN CHANNEL object should be initialized now ...", SEP)
        for (const ws of wsList) {
          let wsInfo = ws.deserializeAttachment() as SessionInfo
          if (DBG0) console.log('state:', ws.readyState, ', url:', ws.url, ', info:', wsInfo)
          // reconstruct things that weren't serialized
          const userKeys = this.visitorKeys.get(wsInfo.userId)!
          _sb_assert(userKeys, "Internal Error [L535]")
          wsInfo = {
            ...wsInfo,
            userKeys: userKeys,
            webSocket: ws,
            ready: true,
            quit: false,
            // receivedUserInfo: false
          }
          this.sessions.set(wsInfo.userId, wsInfo)
          // this.sessions.set(wsInfo.userId, {
          //   userId: wsInfo.userId,
          //   userKeys: userKeys,
          //   channelId: wsInfo.channelId,
          //   isOwner: wsInfo.isOwner,
          //   webSocket: ws,
          //   ready: true,
          //   quit: false,
          //   // receivedUserInfo: false
          // })
        }
        if (DBG0) console.log(SEP, "[RETURNING FROM HIBERNATION] done with post-hibernation work", SEP)
        if (DBG0) console.log(this.sessions)
        resolve(void 0)
      });
      console.log(SEP, "[RETURNING FROM HIBERNATION] 'blockConcurrencyWhile' now awaiting setup finishing", SEP)
      this.state.blockConcurrencyWhile(() => this.hibernationPromise!)
    }
    console.log("++++ ++++ ++++ ++++ ++++ ++++")

    console.log("++++ ChannelServer constructor done ++++")
  }

  // called after a DO channel is initialized. kitchen sink for a variety of
  // tests, debug operations, measurements, and so on. currently it's always
  // called, though what it does and/or prints out will depend on DEBUG level.
  async #startupDebugOrTest() {
    _sb_assert(this.channelId, "ERROR: channelId is not set (fatal)")
    if (dbg.DEBUG2) console.log(SEP, 'Full DO info\n', this, '\n', SEP)
    const _currentMessageKeys = await this.storage.list({ limit: 1000, prefix: this.channelId!, reverse: true });
    if (dbg.DEBUG2) console.log(_currentMessageKeys)
    // these keys are returned as a Map(); we iterate to find lowest and highest (lex) key
    let lowest = '', highest = '';
    // while we are at it, lets accumulate size of the 'values', they're all arraybuffers
    let totalSize = 0, i = 0, metaDataSized = 0 // biggestSoFar = 0
    let foundErrors = false;
    for (const [key, _value] of _currentMessageKeys) {
      i++;
      const value = _value as unknown as ArrayBuffer;
      if (!key) {
        console.error(`ERROR: key for entry ${i} is empty (fatal) (???) `)
        foundErrors = true;
        continue
      }
      if (!lowest || key < lowest) lowest = key;
      if (!highest || key > highest) highest = key;
      if (!value) {
        console.error("ERROR: value is empty (fatal) for key ", key)
        foundErrors = true;
        continue
      }
      if (!((value as any) instanceof ArrayBuffer)) {
        console.error("ERROR: value is not an ArrayBuffer (fatal) for key (?) ", key)
        foundErrors = true;
        continue
      }

      // hard to escape some of the basic components:
      // - iv (12 bytes)
      // - salt (16 bytes)
      // - signature (96 bytes)
      // - two timestamps (string encoded, 13 chars each)
      // - the 'from' identifier (43 chars)

      // comparing a 185-byte message content with 260 bytes, resulting
      // difference in b62 is 976 vs 1077, resulting difference of 101 chars
      // comes from 75 bytes, so 'basic overhead' is about 725 chars, so message
      // can be about 220 bytes. but the 700+ overhead comes from the above 200
      // bytes/char info, so, ToDo: can that be reduced?  if core message can be
      // up to another +100 or 200 bytes more, that will make a major
      // difference. for now we're leaving this be, but it's a future possible
      // improvement: fetching values is limited to 100 (since they can be 32KiB
      // each eg that's 3.2MB), whereas metadata is limited to 1024 characters
      // and ergo 1000 listed values is ~1MB but can be fetched in one go.

      //   const payloadAsMetaData = arrayBufferToBase62(value)
      //   if (payloadAsMetaData.length <= 1000) { // some margin
      //     metaDataSized ++;
      //   }
      //   console.log("Meta data size:", payloadAsMetaData.length, "chars")
      //   if (value.byteLength > biggestSoFar) {
      //   biggestSoFar = value.byteLength;
      //   console.log(SEPx)
      //   // console.log("Biggest so far: ", value.byteLength, "bytes")
      //   const contents = extractPayload(value).payload
      //   console.log(`Biggest so far: ${value.byteLength} bytes (b62 ${payloadAsMetaData.length} chars) (contents ${contents.c.byteLength} bytes)`)
      //   console.log(contents)
      // }

      totalSize += value.byteLength;
    }
    console.log(SEPx)

    // extract the timestamp from 'lowest' and 'highest' keys
    const { timestamp: lowTimestamp } = Channel.deComposeMessageKey(lowest)
    const lowDate = Channel.base4StringToDate(lowTimestamp);
    const { timestamp: highTimestamp } = Channel.deComposeMessageKey(highest);
    const highDate = Channel.base4StringToDate(highTimestamp);

    if (dbg.DEBUG) {
      console.log("\n")
      console.log(SEPx)
      console.log(SEPx)
      console.log("Summary information about DO KV entries (all): ")
      console.log("          ChannelID: ", this.channelId)
      console.log("           Map size: ", _currentMessageKeys.size)
      console.log("    Counted entries: ", i)
      console.log("   (metadata-sized): ", metaDataSized)
      console.log("         Lowest key: ", lowest)
      console.log("         (low date): ", lowDate)
      console.log("        Highest key: ", highest)
      console.log("        (high date): ", highDate)
      console.log(" Total size of data: ", totalSize)
      console.log("       storage left: ", this.storageLimit)
      if (foundErrors) console.log(SEPxStar)
      console.log("       Found errors: ", foundErrors)
      if (foundErrors) console.log(SEPxStar)
      console.log(SEPx)
      console.log("ChannelData:")
      console.log(JSON.stringify(this.channelData, null, 2))
      console.log(SEPx, SEP)
    } else if (foundErrors && dbg.LOG_ERRORS) {
      console.error("ERROR: found errors in DO KV entries in channel " + this.channelId)
    }

    this.latestTimestamp = Channel.base4StringToTimestamp(highTimestamp)
    console.log(
      "\n", SEP,
      "ChannelServer startup, latest timestamp:\n",
      "  number format: ", this.latestTimestamp, "\n",
      "   base4 format: ", highTimestamp, "\n",
      "    date format: ", highDate, "\n",
      SEPx, "\n");
  }

  // load channel from storage: either it's been descheduled, or it's a new
  // channel (that has already been created)
  async #initialize(channelData: SBChannelData) {
    try {
      _sb_assert(channelData && channelData.channelId, "ERROR: no channel data found in parameters (fatal)")
      if (dbg.DEBUG) console.log(`==== ChannelServer.initialize() called for channel: ${channelData.channelId} ====`)
      if (dbg.DEBUG) console.log("\n", channelData, "\n", SEP)

      const channelState = await this.storage.get(
        [
          'channelId',
          'channelData',
          'motherChannel',
          'storageLimit',
          'capacity',
          'latestTimestamp',
          'visitors',
          'locked',
          'messageCount',
          'allMessageCount',
          'newMessageCount',
          'messageHistory',
          // 'ttl0Buffer',
        ]
      )
      const storedChannelData = jsonParseWrapper(channelState.get('channelData') as string, 'L234') as SBChannelData

      if (!channelState || !storedChannelData || !storedChannelData.channelId || storedChannelData.channelId !== channelData.channelId) {
        if (dbg.DEBUG) {
          console.log('ERROR: data missing or not matching:')
          console.log('channelState:\n', channelState)
          console.log('storedChannelData:\n', storedChannelData)
          console.log('channelData:\n', channelData)
        }
        throw new Error('Internal Error [L236]')
      }

      this.storageLimit = Number(channelState.get('storageLimit')) || 0
      this.capacity = Number(channelState.get('capacity')) || 20
      this.latestTimestamp = Number(channelState.get('latestTimestamp')) || 0;
      this.locked = (channelState.get('locked')) === 'true' ? true : false;

      // we do these LAST since it signals that we've fully initialized the channel
      this.channelId = channelData.channelId
      this.channelData = channelData

      this.messageCount = Number(channelState.get('messageCount')) || 0
      this.allMessageCount = Number(channelState.get('allMessageCount')) || 0

      const latestVisitors = channelState.get('visitors')
      if (latestVisitors && latestVisitors instanceof ArrayBuffer) {
        this.visitors = extractPayload(latestVisitors as ArrayBuffer).payload as Map<SBUserId, SBUserPublicKey>
        // now we bootstrap the visitorKeys cache
        for (const [userId, userPublicKey] of this.visitors)
          this.visitorKeys.set(userId, await (new SB384(userPublicKey).ready))      
      } else {
        this.visitors = new Map()
        this.visitorKeys = new Map()
      }
      if (dbg.DEBUG) console.log("Fetched visitors for channel: ", this.visitors)

      this.newMessageCount = Number(channelState.get('newMessageCount')) || 0
      const oldMessageHistory = channelState.get('messageHistory')
      if (oldMessageHistory) {
        this.messageHistory = extractPayload(oldMessageHistory as ArrayBuffer).payload as MessageHistoryDirectory
      } else {
        this.messageHistory = null
      }

      // const oldTTL0Buffer = channelState.get('ttl0Buffer')
      // if (oldTTL0Buffer)
      //   this.ttl0Buffer = new TTL0BufferClass(extractPayload(oldTTL0Buffer as ArrayBuffer).payload as TTL0Buffer)

      // this.refreshCache() // see below

      await this.#startupDebugOrTest()

    } catch (error: any) {
      const msg = `ERROR failed to initialize channel [L250]: ${error.message}`
      if (dbg.LOG_ERRORS) console.error(msg)
      throw new Error(msg)
    }
  }

  // async refreshCache() {
  //   // we prefetch message keys.  we only prefetch/cache perma
  //   // messages for now (eg not subchannels or TTL messages)
  //   const listOptions: DurableObjectListOptions = { limit: serverConstants.MAX_MESSAGE_SET_SIZE, prefix: this.channelId! + '______', reverse: true };
  //   const keys = Array.from((await this.storage.list(listOptions)).keys());
  //   if (keys) this.messageKeysCache = keys // else leave it empty
  //   this.lastCacheTimestamp = this.latestTimestamp; // todo: add a check that ts of last key is same
  //   this.messageKeysCacheMap = new Map(keys.map((key, index) => [key, index]));
  // }

  // this is the fetch picked up by the Durable Object
  async fetch(request: Request) {
    const url = new URL(request.url);
    const path = url.pathname.slice(1).split('/');
    if (!path || path.length < 2) return returnError(request, "Internal Error (L293", 400);
    const channelId = path[0]
    const apiCall = '/' + path[1]

    try {
      if (!dbg.myOrigin) dbg.myOrigin = url.origin;

      if (dbg.DEBUG) console.log("111111 ==== ChannelServer.fetch() ==== phase ONE ==== 111111")
      // phase 'one' - sort out parameters

      // const requestClone = request.clone(); // appears to no longer be needed
      const requestClone = request;

      const apiBody = await processApiBody(requestClone) as Response | ChannelApiBody
      if (apiBody instanceof Response) return apiBody

      if (dbg.DEBUG) {
        console.log(
          SEP,
          '[Durable Object] fetch() called:\n',
          '  myOrigin:', dbg.myOrigin, '\n',
          '    apiCall:', apiCall, '\n',
          '  channelId:', channelId, '\n',
          '  full path:', path, '\n',
          SEP, request.url, '\n', SEP,
          '    apiBody: \n', apiBody, '\n', SEP)
        if (dbg.DEBUG2) console.log(request.headers, '\n', SEP)
      }

      if (dbg.DEBUG) console.log("222222 ==== ChannelServer.fetch() ==== phase TWO ==== 222222")
      // phase 'two' - catch 'create' call (budding against a new channelId)

      if (apiCall === '/budd' && !this.channelId) {
        if (dbg.DEBUG) console.log('\n', SEP, '\n', 'Budding against a new channelId (eg creating/authorizing channel)\n', SEP, apiBody, '\n', SEP)
        return this.#create(requestClone, apiBody);
      }

      // if (and only if) both are in place, they must be consistent
      if (this.channelId && channelId && (this.channelId !== channelId)) return returnError(request, "Internal Error (L478)");

      if (dbg.DEBUG) console.log("333333 ==== ChannelServer.fetch() ==== phase THREE ==== 333333")
      // phase 'three' - check if 'we' just got created, in which case now we self-initialize
      // (we've been created but not initialized if there's no channelId, yet api call is not 'create')
      if (!this.channelId) {
        if (dbg.DEBUG) console.log("**** channel object not initialized ...")
        const channelData = jsonParseWrapper(await (this.storage.get('channelData') as Promise<string>), 'L495') as SBChannelData
        if (!channelData || !channelData.channelId) {
          // no channel, no object, no upload, no dice
          if (dbg.LOG_ERRORS) console.warn(`Channel not initialized, and no channelData in KV, eg does not exist ('${channelId}')`)
          return returnError(request, ANONYMOUS_CANNOT_CONNECT_MSG, 401);
        }
        // channel exists but object needs reloading
        if (channelId !== channelData.channelId) {
          if (dbg.DEBUG) console.log("**** channelId mismatch:\n", channelId, "\n", channelData);
          return returnError(request, "Internal Error (L327)");
        }
        // bootstrap from storage
        await this
          .#initialize(channelData) // it will throw an error if there's an issue
          .catch((e) => {
            if (dbg.LOG_ERRORS) console.error("ERROR: failed to initialize channel (L332)", e)
            return returnError(request, `Internal Error (L332)`);
          });
      }

      if (dbg.DEBUG) console.log("444444 ==== ChannelServer.fetch() ==== phase FOUR ==== 444444")
      // phase 'four' - if we're locked, then only accepted visitors can do anything at all

      if (this.channelId !== path[0])
        return returnError(request, "ERROR: channelId mismatch (?) [L454]");

      // if we're locked, and this is not an owner call, then we need to check if the visitor is accepted
      if (this.locked && !apiBody.isOwner && !this.accepted.has(apiBody.userId)) {
        if (dbg.LOG_ERRORS) console.error("ERROR: channel locked, visitor not accepted")
        return returnError(request, ANONYMOUS_CANNOT_CONNECT_MSG, 401);
      }

      // and if it's not locked, then we keep track of visitors, up to capacity level
      if (!this.locked && !this.visitors.has(apiBody.userId)) {
        // new visitor
        if (!apiBody.userPublicKey)
          return returnError(request, "Need your userPublicKey on this (or prior) operation/message ...", 401);
        if (this.visitors.size >= this.capacity) {
          if (dbg.LOG_ERRORS) console.log(`---- channel ${this.channelId} full, rejecting new visitor ${apiBody.userId}`)
          return returnError(request, ANONYMOUS_CANNOT_CONNECT_MSG, 401);
        }
        this.visitors.set(apiBody.userId, apiBody.userPublicKey)
        await this.storage.put('visitors', assemblePayload(this.visitors))
        this.visitorKeys.set(apiBody.userId, await (new SB384(apiBody.userPublicKey).ready))
      }

      if (dbg.DEBUG) console.log("555555 ==== ChannelServer.fetch() ==== phase FIVE ==== 555555")
      // phase 'five' - every single api call is separately verified with provided visitor public key

      // check signature, check for owner status
      // const sender = await (new SB384(apiBody.userPublicKey).ready)
      const sender = this.visitorKeys.get(apiBody.userId)!
      _sb_assert(sender, "Internal Error [L483]")
      _sb_assert(apiBody.userId === sender.userId, "Internal Error [L484]")

      const viewBuf = new ArrayBuffer(8);
      const view = new DataView(viewBuf);
      view.setFloat64(0, apiBody.timestamp);
      const pathAsArrayBuffer = new TextEncoder().encode(apiBody.path).buffer
      const prefixBuf = _appendBuffer(viewBuf, pathAsArrayBuffer)
      const apiPayloadBuf = apiBody.apiPayloadBuf

      // verification covers timestamp + path + apiPayload
      const verified = await sbCrypto.verify(sender.signKey, apiBody.sign, apiPayloadBuf ? _appendBuffer(prefixBuf, apiPayloadBuf) : prefixBuf)
      if (!verified) {
        if (dbg.LOG_ERRORS) {
          console.error("ERROR: signature verification failed")
          console.log("apiBody:\n", apiBody)
        }
        return returnError(request, ANONYMOUS_CANNOT_CONNECT_MSG, 401);
      }

      // form our own opinion if this is the Owner
      apiBody.isOwner = this.channelId === sender.ownerChannelId

      if (dbg.DEBUG) console.log("666666 ==== ChannelServer.fetch() ==== phase SIX ==== 666666")
      // phase 'six' - we're ready to process the api call!

      // ToDo: verify that the 'embeded' path is same as path coming through in request

      if (apiCall === "/websocket") {
        if (dbg.DEBUG) console.log("==== ChannelServer.fetch() ==== websocket request ====")
        if (dbg.DEBUG) console.log("---- websocket request")
        if (request.headers.get("Upgrade") != "websocket") {
          if (dbg.DEBUG) console.log("---- websocket request, but not websocket (error)")
          return returnError(request, "Expected websocket", 400);
        }
        const ip = request.headers.get("CF-Connecting-IP");
        const pair = new WebSocketPair(); // that's CF websocket pair
        if (dbg.DEBUG) console.log("---- websocket request, creating session")
        await this.#setUpNewSession(pair[1], ip, apiBody); // we use one ourselves
        if (dbg.DEBUG) console.log("---- websocket request, returning session")
        return new Response(null, { status: 101, webSocket: pair[0] }); // we return the other to the client
      } else if (this.ownerCalls[apiCall]) {
        if (dbg.DEBUG) console.log("==== ChannelServer.fetch() ==== owner call ====")
        if (!apiBody.isOwner) {
          if (dbg.LOG_ERRORS) console.log("---- owner call, but not owner (error)");
          return returnError(request, ANONYMOUS_CANNOT_CONNECT_MSG, 401);
        }
        try {
          const result = await this.ownerCalls[apiCall]!(request, apiBody);
          if (dbg.DEBUG) console.log("owner call succeeded")
          return result
        } catch (error: any) {
          console.log("ERROR: owner call failed: ", error)
          console.log(error.stack)
          return returnError(request, `API ERROR [L410] [${apiCall}]: ${error.message} \n ${error.stack}`);
        }
      } else if (this.visitorCalls[apiCall]) {
        if (dbg.DEBUG) console.log("==== ChannelServer.fetch() ==== visitor call ====")
        return await this.visitorCalls[apiCall]!(request, apiBody);
      } else {
        return returnError(request, "API endpoint not found: " + apiCall, 404)
      }
    } catch (error: any) {
      console.trace("ERROR: failed to initialize channel", error)
      console.log(error.stack)
      return returnError(request, `API ERROR [L421] [${apiCall}]: ${error.message} \n ${error.stack}`);
    }
  }

  // #appendMessageKeyToCache(newKey: string): void {
  //   if (this.messageKeysCache.length > 5000) {
  //     // kludgy limiting of state size
  //     const keys = this.messageKeysCache.slice(-500);
  //     // recreate the array and map
  //     this.messageKeysCache = keys
  //     this.messageKeysCacheMap = new Map(keys.map((key, index) => [key, index]));
  //   }
  //   this.messageKeysCache.push(newKey);
  //   this.messageKeysCacheMap.set(newKey, this.messageKeysCache.length - 1);
  // }

  async #incrementMessageCount(perma: boolean): Promise<void> {
    this.messageCount++;
    if (perma) this.allMessageCount++;
    this.newMessageCount++;

    await this.storage.put('messageCount', this.messageCount.toString())
    await this.storage.put('allMessageCount', this.allMessageCount.toString())
    await this.storage.put('newMessageCount', this.newMessageCount.toString()) // in case of interruption

    this.state.setWebSocketAutoResponse(new WebSocketRequestResponsePair('ping', Channel.timestampToBase4String(this.latestTimestamp)))

    // if we are over our preferred limit, we initiate shardification
    if (MESSAGE_HISTORY && this.newMessageCount >= serverConstants.MAX_MESSAGE_SET_SIZE) {
      if (dbg.DEBUG) console.log(`---- initiating shardification .. we have ${this.messageCount} messages`)
      const newHistory = await this.messageCacheToHistoryEntry(this.env)
      // ToDo: add support for depth greater than '0', eg directories of directories etc
      _sb_assert(newHistory && newHistory.depth === 0 && this.messageHistory && this.messageHistory.depth === 0, "Internal Error (L729)")
      if (this.messageHistory) {
        // we have history already, so, we merge the directories
        // ToDo: test this some more
        if (newHistory.shards) {
          if (!this.messageHistory.shards) this.messageHistory.shards = new Map()
          this.messageHistory.shards.set(newHistory.from, newHistory.shards.get(newHistory.from)!)
        }
        this.messageHistory.to = newHistory.to
        this.messageHistory.count += newHistory.count
        this.messageHistory.lastModified = newHistory.lastModified
        await this.storage.put('messageHistory', assemblePayload(this.messageHistory))
        if (dbg.DEBUG) console.log("---- merged (and stored) updated messageHistory:", this.messageHistory)
      } else {
        this.messageHistory = newHistory
        await this.storage.put('messageHistory', assemblePayload(newHistory))
        if (dbg.DEBUG) console.log("---- created (and stored) new message history:", newHistory)
      }
      // reset counter; we do this last, so if we're restarted during the above, should just pick it back up
      this.newMessageCount = 0;
      await this.storage.put('newMessageCount', this.newMessageCount.toString())
    }
  }

  /** all messages come through here. they're either through api call or
      websocket. any issues will throw. for websocket messages,
      'webSocketMessage()' will catch and process any low-level commands
      ('ready', 'close', etc)
  */
  async #processMessage(msg: any, /* userId: SBUserId, */ isOwner: boolean /*, apiBody: ChannelApiBody */): Promise<void> {

    try {
      // var message: ChannelMessage = {}
      // if (typeof msg === "string") {
      //   message = jsonParseWrapper(msg.toString(), 'L594');
      // } else if (msg instanceof ArrayBuffer) {
      //   message = extractPayload(extractPayload(msg).payload).payload // TODO: hack
      // } else {
      //   throw new Error("Cannot parse contents type (not json nor arraybuffer)")
      // }

      if (dbg.DEBUG2) console.log(
        "------------ getting message from client (pre validation) ------------",
        "\n", msg, "\n",
        // "-------------------------------------------------",
        // "\n", "apiBody:\n", apiBody, "\n",
        )

      // if (msg.ready) {
      //   // the client sends a "ready" message when it can start receiving; reception means websocket is all set up
      //   if (DBG0 || dbg.DEBUG2) console.log("got 'ready' message from client, UserId:", /* apiBody. */ userId)
      //   const session = this.sessions.get(/* apiBody. */ userId)
      //   if (!session) {
      //     // if client sends a ready api call, or the socket has closed, we ignore
      //     if (dbg.DEBUG) console.warn("[processMessage]: session not found for 'ready' message, discarding")
      //     if (dbg.DEBUG) console.log(SEP, "Current sessions:\n", this.sessions, SEP)
      //     return;
      //   }
      //   // if there's a session, we mark it as ready and mirror the ready message
      //   session.ready = true;
      //   if (dbg.DEBUG2) console.log("sending 'ready' response to client")
      //   session!.webSocket.send(this.readyMessage());
        
      //   return; // all good
      // }

      // if (msg.reset) {
      //   // client sends 'reset' if it's notices that i might have missed TTL0 messages
      //   const session = this.sessions.get(/* apiBody. */ userId)
      //   if (session) {
      //     // if we have entries in the TTL0 buffer, we send them now
      //     const ttl0Messages = this.ttl0Buffer.messages;
      //     if (ttl0Messages.size > 0) {
      //       const keysToSend = Array.from(ttl0Messages.keys()).sort()
      //       if (DBG0) console.log("ttl0Messages:", ttl0Messages, "keysToSend:", keysToSend)
      //       keysToSend.forEach((key) => {
      //         if (DBG0) console.log("Looking up key:", key)
      //         const msg = ttl0Messages.get(key)
      //         if (!msg) throw new Error("Internal Error (L952), cannot find key: " + key)
      //         if (DBG0) console.log(SEP, "sending TTL0 messages to client", SEP, extractPayload(msg).payload, SEP)
      //         session.webSocket.send(msg);
      //       });
      //     }
      //   }
      // }


      // if (msg.close && msg.close === true) {
      //   if (DBG0 || dbg.DEBUG2) console.log("client is shutting down connection")
      //   const session = this.sessions.get(/* apiBody. */ userId)
      //   if (!session) {
      //     if (dbg.LOG_ERRORS) console.warn("[processMessage]: session not found for 'close' message, ignoring")
      //     return;
      //   }
      //   session.quit = true;
      //   session.webSocket.close(1011, "Client requested 'close'.");
      //   this.sessions.delete(/* apiBody. */ userId);
      //   return; // all good
      // }

      const message: ChannelMessage = validate_ChannelMessage(msg) // will throw if anything wrong
      // if (DBG0) console.log("Message after validation:", message)

      // at this point we have a validated, verified, and parsed message
      // a few minor things to check before proceeding

      if (typeof message.c === 'string') {
        const msg = "[processMessage]: Contents ('c') sent as string. Discarding message."
        if (dbg.DEBUG) console.error(msg)
        throw new Error(msg);
      }

      if (message.i2 && !/* apiBody.*/ isOwner) {
        if (dbg.DEBUG) console.error("ERROR: non-owner message setting subchannel")
        throw new Error("Only Owner can set subchannel. Discarding message.");
      } else if (!message.i2) {
        message.i2 = '____' // default; use to keep track of where we're at in processing
      }

      const tsNum = Math.max(Date.now(), this.latestTimestamp + 1);
      message.sts = tsNum; // server timestamp
      this.latestTimestamp = tsNum;
      await this.storage.put('latestTimestamp', tsNum)

      // appending timestamp to channel id. this is global, unique message identifier
      // const key = this.channelId + '_' + message.i2 + '_' + ts;
      const key = Channel.composeMessageKey(this.channelId!, tsNum, message.i2)

      // TODO: sync TTL with 'i2'
      var i2Key: string | null = null

      if (message.ttl && message.ttl > 0 && message.ttl < 0xF) {
        // these are the cases where messages are also stored under a TTL subchannel
        if (message.i2[3] === '_') {
          // if there's a TTL, you can't have a subchannel using last character, this is an error
          if (dbg.DEBUG) console.error("ERROR: subchannel cannot be used with TTL")
          throw new Error("Subchannel cannot be used with TTL. Discarding message.")
        } else {
          // ttl is a digit 3-8, we append that to the i2 centerpiece (eg encoded in subchannel)
          // i2Key = this.channelId + '_' + message.i2.slice(0, 3) + message.ttl + '_' + ts;
          i2Key = Channel.composeMessageKey(this.channelId!, tsNum, message.i2.slice(0, 3) + message.ttl)
        }
      }

      // messages with destinations must be short-lived
      if (!message.ttl || message.ttl > 0x8) { // max currently defined message age, 10 days
        if (message.t)
          throw new Error("ERROR: any 'to' (routed) message must have a short TTL")
      }

      // strip (minimize) it and, package it, result chunk is stand-alone except
      // for channelId; calling in 'serverMode' (true) to enforce server-side
      const messagePayload = assemblePayload(stripChannelMessage(message, true))!

      // finally, we enforce storage limit
      const messagePayloadSize = messagePayload.byteLength
      if (messagePayloadSize > serverConstants.MAX_SB_BODY_SIZE) {
        if (dbg.DEBUG) console.error(`ERROR: message too large (${messagePayloadSize} bytes)`)
        throw new Error("Message too large. Discarding message.");
      }

      if (dbg.DEBUG) {
        const payloadAsMetaData = arrayBufferToBase62(messagePayload)
        if (payloadAsMetaData.length <= 1000) { // some margin
          console.log(SEPx)
          // we're tracking this as 'fyi' for near future optimization
          console.log(`[info] this message could be stored as metadata (size would be ${messagePayloadSize} chars)`)
          console.log(payloadAsMetaData.slice(0, 200) + "..." + payloadAsMetaData.slice(-50))
          console.log(SEPx)
        }
      }

      // consume storage budget; 'TTL0' handling
      const multiplier = message.ttl === 0 ? CHANNEL_STORAGE_MULTIPLIER_TEMP : CHANNEL_STORAGE_MULTIPLIER_PERMA
      const spaceNeeded = messagePayloadSize * multiplier
      // if (spaceNeeded > this.storageLimit) {
      //   if (dbg.LOG_ERRORS) console.error(`ERROR: storage limit (${this.storageLimit}) exceeded, need ${spaceNeeded} bytes.`)
      //   throw new Error(`Storage limit exceeded. Message cannot be stored or broadcast. Discarding message. Storage amount needed: ${spaceNeeded} bytes.`);
      // }
      // this.storageLimit -= (messagePayloadSize * multiplier);
      // await this.storage.put('storageLimit', this.storageLimit)

      await this.#consumeStorage(spaceNeeded, false); // don't need results

      // todo: add per-user budget limits; also adjust down cost for TTL, especially '0'

      // if (DBG0) console.log("Message so far:", message)
      if (message.ttl !== 0) {
        await this.storage.put(key, messagePayload); // local storage
        await this.env.MESSAGES_NAMESPACE.put(key, messagePayload); // global storage
        if (i2Key) {
          // we don't block on these (messages with TTLs have slightly lower SLA)
          this.storage.put(i2Key, messagePayload);
          this.env.MESSAGES_NAMESPACE.put(i2Key, messagePayload);
        } else {
          // // and currently it's only non-subchannel messages that we cache
          // this.#appendMessageKeyToCache(key);
        }
      }

      // everything looks good. we broadcast to all connected clients (if any)
      await this.#broadcast(messagePayload)

      // all messages are cached in the 'TTL0' buffer
      this.ttl0Buffer.addMessage(key, messagePayload, message.t)

      // after broadcast, we update the message count
      await this.#incrementMessageCount(!message.ttl || message.ttl === 0xF)
    } catch (error: any) {
      if (dbg.LOG_ERRORS) console.error("ERROR: failed to process message", error)
      throw new SBError("Failed to process message: " + error.message)
    }

  }

  async #send(request: Request, apiBody: ChannelApiBody) {
    // _sb_assert(apiBody && apiBody.apiPayload, "[ChannelServer] send(): need payload (the message)")
    if (!apiBody || !apiBody.apiPayload) {
      if (dbg.LOG_ERRORS) console.error("[ChannelServer.send] No payload found?")
      return returnError(request, "No payload included - 'send' needs payload (...perhaps a missing 'await' at your end?)");
    }
    if (dbg.DEBUG) {
      console.log("==== ChannelServer.handleSend() called ====")
      console.log(apiBody)
    }
    try {
      await this.#processMessage(apiBody.apiPayload!, /* apiBody.userId, */ apiBody.isOwner! /*, apiBody */)
      return returnSuccess(request)
    } catch (error: any) {
      return returnError(request, error.message, 400);
    }
  }

  async #setPage(request: Request, apiBody: ChannelApiBody) {
    if (dbg.DEBUG) {
      console.log('\n', "==== ChannelServer.setPage() called ====")
      console.log(apiBody)
      console.log(apiBody.apiPayload)
    }
    const ownerKeys = this.visitorKeys.get(apiBody.userId)!
    _sb_assert(ownerKeys, "Internal Error [L615]")
    let { page, type } = apiBody.apiPayload as { page: any, type: string }
    _sb_assert(apiBody && apiBody.apiPayload && page, "[setPage] parameters missing or invalid (fatal)")
    if (!type) type = 'sb384payloadV3' // magical default
    if (type === 'sb384payloadV3')
      page = assemblePayload(page) // we need to package it

    // de facto, at this point 'page' is either a string or ArrayBuffer
    // if needed we can add support for some other types (eg Blog)
    let pageSize = 0
    let hashBufferSource: ArrayBuffer
    if (typeof page === 'string') {
      pageSize = page.length
      hashBufferSource = new TextEncoder().encode(page).buffer
    } else if (page instanceof ArrayBuffer) {
      pageSize = page.byteLength
      hashBufferSource = page
    } else if (page instanceof Uint8Array) {
      page = page.buffer
      pageSize = page.byteLength
      hashBufferSource = page
    } else {
      if (dbg.DEBUG) console.error("Got contents we can't process: ", page)
      return returnError(request, `Contents provided for Page not supported (needs to be string or ArrayBuffer)`);
    }

    if (pageSize > serverConstants.STORAGE_SIZE_MAX) {
      return returnError(request, `Page contents too large (${pageSize} bytes, current limit is ${serverConstants.STORAGE_SIZE_MAX} bytes)`, 413);
    }

    if (dbg.DEBUG) console.log(page, type, '\n', SEP)
    try {
      const size = await this.#consumeStorage(pageSize! * CHANNEL_STORAGE_MULTIPLIER_PERMA);
      const pageKey = ownerKeys.hashB32
      const pageKeyKV = genKey(pageKey, 'G')
      // first we sanity check that this does not exist or, if it does, it's the
      // same owner (channel) as 'us)
      const { value, metadata } = await this.env.PAGES_NAMESPACE.getWithMetadata(pageKeyKV, { type: "arrayBuffer" });
      if (value) {
        if (dbg.DEBUG) console.log("Checked for existing entry, got this: ", value, metadata)
        const existingOwner = (metadata as PageMetaData).owner
        if (existingOwner !== ownerKeys.userPublicKey) {
          if (dbg.DEBUG) {
            console.error("ERROR: page already exists and is owned by someone else")
            console.log("ERROR: existing owner: ", existingOwner)
            console.log("ERROR: callee        : ", ownerKeys.userPublicKey)
          }
          throw new SBError("Page already exists and is owned by someone else")
        } else {
          if (dbg.DEBUG) console.log("Page already exists and is owned by us, overwriting current page")
        }
      }

      // we want to generate a hash of page for use with Etag
      // we want to use the subtle crypto standard api with sha-256 hashing
      const hashBuffer = await crypto.subtle.digest('SHA-256', hashBufferSource)
      const hashBufferString = arrayBufferToBase62(hashBuffer)

      const pageMetaData: PageMetaData = {
        id: pageKey,
        owner: ownerKeys.userPublicKey,
        size: size,
        lastModified: Date.now(), // this refers to the Page, not contents
        hash: hashBufferString,
        shortestPrefix: apiBody.apiPayload.shortestPrefix || 6, // defaults to shortest
        type: type, // MIME type, if present then pre-process and 'type' the response
      }
      // todo/consider: do we want to inject any of server-side meta data into the page payload?
      await this.env.PAGES_NAMESPACE.put(pageKeyKV, page, { metadata: pageMetaData });
      if (dbg.DEBUG) console.log("Putting this object onto this Page key: ", pageMetaData, pageKeyKV, page)
      return returnResult(request, { success: true, pageKey: pageKey, size: size })
    } catch (error: any) {
      if (error instanceof SBError)
        return returnError(request, '[setPage] ' + error.message, 400);
      if (dbg.LOG_ERRORS) console.error("ERROR: failed to set page: ", error.message)
      return returnError(request, ANONYMOUS_CANNOT_CONNECT_MSG, 401);
    }
  }

  // this is called directly from the Durable Object for a new message
  async webSocketMessage(ws: WebSocket, msg: string | ArrayBuffer) {
    _sb_assert(msg, "[ChannelServer.webSocketMessage] ERROR: no message received (fatal)")
    if (dbg.DEBUG2) console.log("==== ChannelServer.webSocketMessage() called ====\n", msg)
    try {
      if (this.hibernationPromise) await this.hibernationPromise
      const userId = this.state.getTags(ws)[0]!;
      const session = this.sessions.get(userId)
      if (!session) {
        ws.close(1011, "[ChannelServer.webSocketMessage] No current session (closing socket), userId: " + userId);
        if (DBG0 || dbg.LOG_ERRORS) console.log("[ChannelServer.webSocketMessage]: No sesssion for: ", userId)
      } else if (session.quit) {
        ws.close(1011, "[ChannelServer.webSocketMessage] The session has been closed (quit) for userId: " + userId);
        this.sessions.delete(userId);
      } else {
        if (typeof msg === "string") {
          // the channel server responds directly to any string type messages,
          // these are 'link level' (kind of).
          const regex = /^[0-3]{26}$/;
          if (regex.test(msg)) {
            // if it's a (timestamp) prefix string, then it's a request for TTL0 buffer
            // if we're asked for something that is before our oldest TTL0 message, then we
            // disconnect
            if (this.ttl0Buffer.bufferOldestTimestamp > msg && msg !== '0'.repeat(26)) {
              if (DBG0) console.log(
                SEP,
                "Disconnecting client, requested TTL0 buffer is too old",
                SEP,
                "requested:", msg,
                "oldest:   ", this.ttl0Buffer.bufferOldestTimestamp,
                SEP
                )
              ws.close(1011, "Requested TTL0 buffer is too old.");
              session.quit = true;
              this.sessions.delete(userId);
            } else {
              // we iterate through TTL0 buffer and send all messages with timestamp > prefix
              // console.log("CALLING iterate with msg:", msg)
              this.ttl0Buffer.iterate((_key, userId, message) => {
                if (!userId || userId !== userId || session.isOwner) {
                  if (DBG0) console.log(SEP, "sending TTL0 (and other) messages to client", SEP, extractPayload(message).payload, SEP)
                  ws.send(message);
                }
                return true; // continue
              }, msg);
            }
          } else switch (msg) {
            case 'close':
              // client sends 'close' if it's shutting down connection
              if (DBG0 || dbg.DEBUG) console.log("client is shutting down connection")
              session.quit = true;
              ws.close(1011, "Client requested 'close'.");
              this.sessions.delete(userId);
              return;
            case 'ping':
              // 'keep alive' message from client, respond with timestamp prefix; if running on
              // hibernation-capable network stack, this is caught before getting here
              if (DBG0 || dbg.DEBUG) console.log("[ChannelServer.webSocketMessage] Sending ping response to client")
              ws.send(Channel.timestampToBase4String(this.latestTimestamp))
              return;
            case 'ready':
              // client sends a "ready" message when it can start receiving; reception means websocket is all set up
              if (DBG0 || dbg.DEBUG) console.log("got 'ready' message from client, UserId:", userId)
              session.ready = true;
              ws.send(this.readyMessage());
              return;
            default:
              if (DBG0 || dbg.DEBUG) console.log("Got string message from client, but not recognized: ", msg)
              ws.send(assemblePayload({ error: "[ChannelServer.webSocketMessage] Message not recognized (fatal): " + msg })!);
              return;
          }
        } else {
          const message = extractPayload(msg as ArrayBuffer).payload
          if (!message)
            ws.send(assemblePayload({ error: "[ChannelServer.webSocketMessage] Could not process message payload" })!);
          else
            await this.#processMessage(message, /* userId, */ session.isOwner /*, apiBody */);
        }
      }
    } catch (error: any) {
      if (dbg.LOG_ERRORS) console.error('[ChannelServer.webSocketMessage] Unknown error while processing message:\n', error.message)
      ws.send(assemblePayload({ error: '[ChannelServer.webSocketMessage] Unknown error while processing message' })!);
    }
  }

  readyMessage() {
    return assemblePayload({
      ready: true,
      messageCount: this.messageCount,
      latestTimestamp: Channel.timestampToBase4String(this.latestTimestamp),
     })!;
  }

  async #setUpNewSession(webSocket: WebSocket, _ip: string | null, apiBody: ChannelApiBody) {
    _sb_assert(webSocket && (webSocket as any).accept, "ERROR: webSocket does not have accept() method (fatal)");

    // (webSocket as any).accept(); // typing override (old issue with CF types)

    if (DBG0) console.log("Setting up new websocket session for user: ", apiBody.userId)
    this.state.acceptWebSocket(webSocket, [apiBody.userId])

    // attach (serializable) subset of info to websocket
    let sessionHibernate: SessionInfoHibernated = {
      userId: apiBody.userId,
      channelId: apiBody.channelId,
      isOwner: apiBody.isOwner === true,
    }
    webSocket.serializeAttachment(sessionHibernate)

    // not applicable to CF 'webSocket' object ?
    // webSocket.binaryType = "arraybuffer"; // otherwise default is 'blob'

    const userKeys = this.visitorKeys.get(apiBody.userId)!
    _sb_assert(userKeys, "Internal Error [L1364]")

    // Create our session and add it to the sessions list.
    let session: SessionInfo = {
      ...sessionHibernate,
      userKeys: userKeys,
      webSocket: webSocket,
      ready: false,
      quit: false, // tracks cleanup, true means go away
    };

    // if same listener is already active, we close it; note we try to follow
    // https://www.rfc-editor.org/rfc/rfc6455.html#section-7.4.1
    if (this.sessions.has(apiBody.userId)) {
      if (DBG0 || dbg.DEBUG) console.log("closing previous session for same user")
      this.sessions.get(apiBody.userId)!.webSocket!.close(1011, "Same user/key has opened a new session; closing this one.");
    }

    // track active connections
    this.sessions.set(apiBody.userId, session)

    // webSocket.addEventListener("message", async msg => {
    //   try {
    //     if (session.quit) {
    //       webSocket.close(1011, "WebSocket broken (got a quit).");
    //       return;
    //     }
    //     try {
    //       const message = extractPayload(msg.data).payload
    //       if (!message) throw new Error("ERROR: could not process message payload")
    //       await this.#processMessage(message, apiBody); // apiBody from original setup
    //     } catch (error: any) {
    //       console.error(`[channel server websocket listener] error: <<${error.message}>>`)
    //       // console.log(msg)
    //       console.log(msg.data)
    //       webSocket.send(JSON.stringify({ error: `ERROR: failed to process message: ${error.message}` }));
    //     }
    //   } catch (error: any) {
    //     // Report any exceptions directly back to the client
    //     const err_msg = '[handleSession()] ' + error.message + '\n' + error.stack + '\n';
    //     if (dbg.DEBUG2) console.log(err_msg);
    //     try {
    //       webSocket.send(JSON.stringify({ error: err_msg }));
    //     } catch {
    //       console.error(`ERROR: was unable to propagate error to client: ${err_msg}`);
    //     }
    //   }
    // });

    // // On "close" and "error" events, remove matching sessions
    // const closeOrErrorHandler = () => {
    //   session.quit = true; // tells any closure to go away
    //   // this.sessions = this.sessions.filter(member => member !== session);
    //   this.sessions.delete(apiBody.userId)
    // };

    // webSocket.addEventListener("close", closeOrErrorHandler);
    // webSocket.addEventListener("error", closeOrErrorHandler);

    // just a little ping that we're up and running
    webSocket.send(this.readyMessage());
  }

  async webSocketClose(ws: WebSocket /*, code: number, reason: string, wasClean: boolean */) {
    const userId = this.state.getTags(ws)[0]!;
    const session = this.sessions.get(userId)
    if (session) {
      session.quit = true;
      this.sessions.delete(userId)
    }
  }

  async webSocketError(ws: WebSocket, error: Error) {
    console.error("WebSocket error:", error.message);
    const userId = this.state.getTags(ws)[0]!;
    const session = this.sessions.get(userId)
    if (session) {
      session.quit = true;
      this.sessions.delete(userId)
    }
  }

  // broadcasts a message to all clients.
  async #broadcast(messagePayload: ArrayBuffer) {
    if (this.sessions.size === 0) return; // nothing to do
    // MTG TODO: should we send notifications for everything? for example locked-out messages?
    if (dbg.DEBUG2) console.log("calling sendWebNotifications()", messagePayload);
    await this.#sendWebNotifications(); // ping anybody subscribing to channel
    // Iterate over all the sessions sending them messages.
    this.sessions.forEach((session, _userId) => {
      if (session.ready) {
        try {
          if (dbg.DEBUG2) console.log("sending message to session (user): ", session.userId)
          session.webSocket.send(messagePayload);
          if (dbg.DEBUG2) console.log(SEP, "sent BROADCAST message to client", SEP, extractPayload(messagePayload).payload, SEP)
        } catch (err) {
          if (dbg.DEBUG) console.log("ERROR: failed to send message to session: ", session.userId)
          session.ready = false;
          session.quit = true; // should probably close it
        }
      } else {
        if (dbg.DEBUG) console.warn(`session not ready, not forwarding message to ${session.userId}`);
      }
    }
    );
  }

  timestampRegex = /^[0-3]*$/;

  // gets as many keys as possible
  async #_getMessageKeys(prefix: string) {
    const listOptions: DurableObjectListOptions = {
      limit: serverConstants.MAX_MESSAGE_SET_SIZE * 2, // we allow more, in case there's some racing 
      prefix: this.channelId! + '______' + prefix,
      reverse: true,
      allowConcurrency: true,
    };
    return await this.storage.list(listOptions)
  }

  async #getMessageKeys(request: Request, apiBody: ChannelApiBody) {
    // ToDo: carry forward these options from handleOldMessages()
    //       (using abiBody format)
    // const { searchParams } = new URL(request.url);
    // const currentMessagesLength = Number(searchParams.get('currentMessagesLength')) || 100;
    // const cursor = searchParams.get('cursor') || '';

    // // main cache should always be in sync
    // if (this.lastCacheTimestamp !== this.latestTimestamp) {
    //   if (dbg.LOG_ERRORS) console.log("**** message cache is out of sync, reloading it ...")
    //   this.refreshCache()
    // }

    // if (this.lastL1CacheTimestamp !== this.latestTimestamp) {
    //   // L1 cache is not up-to-date, update it from the L2 cache
    //   this.recentKeysCache = new Set(this.messageKeysCache.slice(-100));
    // }
    // this.lastL1CacheTimestamp = this.latestTimestamp;
    // return returnResult(request, this.recentKeysCache)

    // 2024.02.20 yet another redesign ...

    const prefix = apiBody.apiPayload?.prefix || ''
    if (prefix && !this.timestampRegex.test(prefix))
      return returnError(request, "Invalid timestamp prefix (must be [0-3]*)", 400);

    const listQuery = await this.#_getMessageKeys(prefix)

    if (listQuery.size > serverConstants.MAX_MESSAGE_SET_SIZE) {
      if (dbg.LOG_ERRORS) console.error("ERROR: message set too large")
      return returnError(request, "Message set too large (need to add pagination)", 400);
    }

    // // we check for any matches in ttl0 buffer
    // const ttl0Messages = this.ttl0Buffer.getMessageKeys(prefix)
    // // merge the sets
    // const allKeys = new Set([...listQuery.keys(), ...ttl0Messages])

    if (listQuery && listQuery.size > 0) {
      // we get a Map, but we want to return a Set using the keys
      const keys = new Set(listQuery.keys())
      return returnResult(request, { keys: keys })
    } else {
      return returnResult(request, { keys: new Set() })
    }

    // if (keys) this.messageKeysCache = keys // else leave it empty
    // this.lastCacheTimestamp = this.latestTimestamp; // todo: add a check that ts of last key is same
    // this.messageKeysCacheMap = new Map(keys.map((key, index) => [key, index]));

  }

  // return known set of userId (hash) -> public key
  async #getPubKeys(request: Request, _apiBody: ChannelApiBody) {
    return returnResult(request, this.visitors)
  }

  // getlatestTimestamp
  #getLatestTimestamp(request: Request, _apiBody: ChannelApiBody) {
    return returnResult(request, Channel.timestampToBase4String(this.latestTimestamp))
  }

  // async function storageFetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response>{
  //   console.log("Fetching from storage server: ", input)
  //   // if the input has "<CHANNEL_SERVER_REDIRECT>" in it, then it's meant for *this* worker
  //   // and we redirect it to ourselves; we do that by first stripping that string,
  //   // and then calling 'serferFetch' with the new URL
  //   if (typeof input === 'string') {
  //     if (input.includes("<CHANNEL_SERVER_REDIRECT>")) {
  //       input = input.replace("<CHANNEL_SERVER_REDIRECT>", "")
  //       return serverFetch(input, init)
  //     }
  //   }
  //   const response = await storageServerBinding.fetch(input, init)
  //   console.log("Got response from storage server: ", response)
  //   return response
  // }


  createCustomFetch(env: EnvType) {
    // This function returns a new function that is "fetch-like"
    return async function (input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
      console.log("Custom fetch called with: ", input);
      if (typeof input === 'string' && input.includes("<CHANNEL_SERVER_REDIRECT>")) {
        _sb_assert(dbg.myOrigin, "Our origin still unknown. Internal Error [L1238]")
        input = dbg.myOrigin + input.replace("<CHANNEL_SERVER_REDIRECT>", "");
        const request = new Request(input, init);
        const result = await serverFetch(request, env); // closure over env
        console.log("Got response from ourselves: ", result);

        // let's 'clone' the result, and print out the body
        const clonedResult = result.clone();
        // let's print out content type
        console.log("Content type: ", clonedResult.headers.get('content-type'));
        // if it's a text type, including json, print it as text
        if (clonedResult.headers.get('content-type')?.includes('text') || clonedResult.headers.get('content-type')?.includes('json')) {
          const text = await clonedResult.text();
          console.log("Body: ", text);
        }
        // if it's binary, let's print out first 100 bytes in hexadecimals
        if (clonedResult.headers.get('content-type')?.includes('application/octet-stream')) {
          const buffer = await clonedResult.arrayBuffer();
          const hex = Array.from(new Uint8Array(buffer)).map(b => b.toString(16).padStart(2, '0')).join('');
          console.log("Body (hex): ", hex.slice(0, 200) + "...");
          console.log(SEP, "Payload: \n", extractPayload(buffer).payload, SEP)
        }

        return result;
      }
      // ToDo: confirm it's to the storage server
      if (env.IS_LOCAL) {
        console.log(SEP, "Running LOCAL. We just relay the fetch ('development'):\n", input, SEP)
        return fetch(input, init);
      } else {
        console.log(SEP, "Deployed. We redirect storage API calls to service binding:\n", input, SEP)
        const response = await storageServerBinding.fetch(input, init);
        console.log("Got response from storage server (NON LOCAL): ", response);
        return response;
      }
    };
  }

  // for testing - WORKS!
  // async getStorageServerInfo(env: EnvType): Promise<any> {
  //   if (!storageFetchFunction) storageFetchFunction = this.createCustomFetch(env);
  //   if (!storageServerBinding) storageServerBinding = env.STORAGE_SERVER_BINDING
  //   // const response = await env.STORAGE_SERVER_BINDING.fetch(env.STORAGE_SERVER_NAME + "/info")
  //   console.log("Getting storage server info")
  //   try {
  //     const sb = new Snackabra("<CHANNEL_SERVER_REDIRECT>", { sbFetch: storageFetchFunction })
  //     const storageServer = await sb.storage.getStorageServer()
  //     console.log("Got response from storage server: ", storageServer)
  //     return storageServer
  //   } catch (error: any) {
  //     console.error("ERROR: failed to get storage server info: ", error)
  //     return ({ error: error.message })
  //   }
  // }

  // // for testing - WORKS!
  // async storeRandomBuffer(env: EnvType): Promise<SBObjectHandle | void> {
  //   if (!storageFetchFunction) storageFetchFunction = this.createCustomFetch(env);
  //   if (!storageServerBinding) storageServerBinding = env.STORAGE_SERVER_BINDING
  //   // const response = await env.STORAGE_SERVER_BINDING.fetch(env.STORAGE_SERVER_NAME + "/info")
  //   console.log("Getting storage server info")
  //   try {
  //     const sb = new Snackabra("<CHANNEL_SERVER_REDIRECT>", { sbFetch: storageFetchFunction, DEBUG: true })

  //     const testSize = 63 * 1024
  //     const token = this.#generateStorageToken(testSize) // note: we do *not* charge for this
  //     await this.env.LEDGER_NAMESPACE.put(token.hash, JSON.stringify(token))
  //     const testBlock = crypto.getRandomValues(new Uint8Array(testSize))
  //     const shardHandle = await sb.storage.storeData(testBlock, token)

  //     await shardHandle.verification
  //     console.log('\n', SEP, "Reply from 'SB.storage.storeData()': \n", shardHandle, '\n', SEP)

  //     return shardHandle
  //   } catch (error: any) {
  //     console.error("ERROR: failed to get storage server info: ", error)
  //   }
  // }

  // // for testing - WORKS!  (this is just with KEYS)
  // async storeMessageCache(env: EnvType): Promise<SBObjectHandle | void> {
  //   if (!storageFetchFunction) storageFetchFunction = this.createCustomFetch(env);
  //   if (!storageServerBinding) storageServerBinding = env.STORAGE_SERVER_BINDING
  //   try {
  //     const sb = new Snackabra("<CHANNEL_SERVER_REDIRECT>", { sbFetch: storageFetchFunction, DEBUG: true })

  //     const cache = assemblePayload(this.messageKeysCache)
  //     const token = this.#generateStorageToken(cache!.byteLength)
  //     await this.env.LEDGER_NAMESPACE.put(token.hash, JSON.stringify(token))
  //     const shardHandle = await sb.storage.storeData(cache, token)

  //     await shardHandle.verification
  //     console.log('\n', SEP, "Reply from 'SB.storage.storeData()': \n", shardHandle, '\n', SEP)

  //     return shardHandle
  //   } catch (error: any) {
  //     console.error("ERROR: failed to get storage server info: ", error)
  //   }
  // }

  // // for testing - THIS ALSO WORKED, and is immediate basis for messageCacheToHistoryEntry()
  // async storeMessageCache(env: EnvType): Promise<SBObjectHandle> {
  //   if (!storageFetchFunction) storageFetchFunction = this.createCustomFetch(env);
  //   if (!storageServerBinding) storageServerBinding = env.STORAGE_SERVER_BINDING
  //   try {
  //     const sb = new Snackabra("<CHANNEL_SERVER_REDIRECT>", { sbFetch: storageFetchFunction, DEBUG: env.DEBUG_ON })

  //     // const messageHistory = new Map<string, ArrayBuffer>();
  //     // const keys = this.messageKeysCache;
  //     // const getOptions: DurableObjectGetOptions = { allowConcurrency: true };
  //     // let remainingKeys = keys.length;

  //     // while (keys.length > 0) {
  //     //   const batchKeys = keys.splice(-100);
  //     //   const results = await this.storage.get(batchKeys, getOptions);
  //     //   for (const [key, { value }] of Object.entries(results)) {
  //     //     if (value) messageHistory.set(key, value);
  //     //     remainingKeys--;
  //     //   }
  //     // }

  //     const messageHistory = new Map<string, ArrayBuffer>();

  //     // const keys = [...this.messageKeysCache]; // Clone to avoid mutating the original array
  //     const keysMap = await this.#_getMessageKeys('0')
  //     // turn the keys into an array
  //     const keys = Array.from(keysMap.keys())

  //     console.log("0000 0000 [storeMessageCache] Got keys: ", keys)

  //     const getOptions: DurableObjectGetOptions = { allowConcurrency: true };

  //     let lowestFrom = Channel.LOWEST_TIMESTAMP
  //     let highestTo = Channel.HIGHEST_TIMESTAMP
  //     while (keys.length > 0) {
  //       // '100' is max number of values we can get in one go from CF storage
  //       const batchKeys = keys.splice(0, 100);
  //       const results: Map<string, ArrayBuffer> = await this.storage.get(batchKeys, getOptions)
  //       if (results instanceof Map) {
  //         for (const [key, value] of results) {
  //           if (value) {
  //             messageHistory.set(key, value);
  //             if (key > lowestFrom) lowestFrom = key
  //             if (key < highestTo) highestTo = key
  //           }
  //         }
  //       } else {
  //         throw new SBError("Unexpected result format from storage.get, fatal (Internal Error L1407)");
  //       }
  //     }

  //     console.log("1111 1111 [storeMessageCache] Got messageHistory: ", messageHistory)

  //     const messageHistoryPayload = assemblePayload(messageHistory)!
  //     const token = this.#generateStorageToken(messageHistoryPayload.byteLength);

  //     await this.env.LEDGER_NAMESPACE.put(token.hash, JSON.stringify(token))

  //     console.log("2222 2222 [storeMessageCache] About to store with payload")

  //     // const shardHandle = await sb.storage.storeData(messageHistoryPayload, token)
  //     const shardHandle = await sb.storage.storeData(messageHistory, token)

  //     console.log("3333 3333 [storeMessageCache] Got handle in return:\n", shardHandle)

  //     await shardHandle.verification
  //     console.log('\n', SEP, "Reply from 'SB.storage.storeData()': \n", shardHandle, '\n', SEP)

  //     this.historyShard = shardHandle

  //     return shardHandle
  //   } catch (error: any) {
  //     console.error("ERROR: failed to get storage server info: ", error)
  //   }
  // }

  /**
   * Takes current message cache, and constructs a history entry from it.
   */
  async messageCacheToHistoryEntry(env: EnvType): Promise<MessageHistoryDirectory> {
    _sb_assert(MESSAGE_HISTORY, "MESSAGE_HISTORY not set, fatal")
    if (!storageFetchFunction) storageFetchFunction = this.createCustomFetch(env);
    if (!storageServerBinding) storageServerBinding = env.STORAGE_SERVER_BINDING
    try {
      const sb = new Snackabra("<CHANNEL_SERVER_REDIRECT>", { sbFetch: storageFetchFunction, DEBUG: env.DEBUG_ON })
      const messageHistory = new Map<string, ArrayBuffer>();
      const keysMap = await this.#_getMessageKeys('0')
      const keys = Array.from(keysMap.keys())
      console.log("0000 0000 [storeMessageCache] Got keys: ", keys)
      const getOptions: DurableObjectGetOptions = { allowConcurrency: true };
      let lowestFrom = Channel.LOWEST_TIMESTAMP
      let highestTo = Channel.HIGHEST_TIMESTAMP
      while (keys.length > 0) {
        // '100' is max number of values we can get in one go from CF storage
        const batchKeys = keys.splice(0, 100);
        const results: Map<string, ArrayBuffer> = await this.storage.get(batchKeys, getOptions)
        // global doesn't support fetchin with a batch of keys
        // const results: Map<string, ArrayBuffer> = await this.env.MESSAGES_NAMESPACE.get(batchKeys, getOptions)
        if (results instanceof Map) {
          for (const [key, value] of results) {
            if (value) {
              messageHistory.set(key, value);
              const { timestamp } = Channel.deComposeMessageKey(key)
              if (timestamp > lowestFrom) lowestFrom = key
              if (timestamp < highestTo) highestTo = key
            }
          }
        } else {
          throw new SBError("Unexpected result format from storage.get, fatal (Internal Error L1407)");
        }
      }
      console.log("1111 1111 [storeMessageCache] Got messageHistory: ", messageHistory)
      let lastModified, created = lastModified = Date.now();
      const historyEntry: MessageHistoryEntry = {
        type: 'entry',
        version: '20240228001',
        channelId: this.channelId!,
        ownerPublicKey: this.channelData!.ownerPublicKey,
        created: created,
        from: lowestFrom,
        to: highestTo,
        count: messageHistory.size,
        messages: messageHistory,
      }
      const messageEntryPayload = assemblePayload(historyEntry)!
      const token = this.#generateStorageToken(messageEntryPayload.byteLength);
      // authorize the storage
      await this.env.LEDGER_NAMESPACE.put(token.hash, JSON.stringify(token))
      console.log("2222 2222 [storeMessageCache] About to store with payload")
      const shardHandle = await sb.storage.storeData(messageEntryPayload, token)
      console.log("3333 3333 [storeMessageCache] Got handle in return:\n", shardHandle)
      await shardHandle.verification
      console.log('\n', SEP, "Reply from 'SB.storage.storeData()': \n", shardHandle, '\n', SEP)
      const historyDirectory: MessageHistoryDirectory = {
        ...historyEntry,
        depth: 0, // indicates all entries (in this case just the one) are shards of message maps
        type: 'directory',
        lastModified: lastModified,
        shards: new Map<string, SBObjectHandle>().set(lowestFrom, shardHandle),
      }
      //@ts-ignore
      delete historyDirectory.messages // hack to work around TS type limitations
      return historyDirectory
    } catch (error: any) {
      throw new SBError("ERROR: failed to get storage server info: " + error)
    }
  }

  async #getHistory(request: Request, _apiBody: ChannelApiBody) {
    _sb_assert(MESSAGE_HISTORY, "MESSAGE_HISTORY not set, fatal")

    // const testInfo = await this.getStorageServerInfo(this.env)
    // console.log("Got storage server info: ", testInfo)
    // return returnResult(request, { storageInfo: testInfo, greeting: 'hello from /getHistory' })
    // const shardHandle = await this.storeRandomBuffer(this.env)

    // const shardHandle = await this.storeMessageCache(this.env)
    // if (!shardHandle) return returnError(request, "Failed to store random buffer", 400)

    // // return returnResult(request, { shardHandle: await stringify_SBObjectHandle(shardHandle), greeting: 'hello from /getHistory' })

    // return returnResult(request, { shardHandle: shardHandle, greeting: 'hello from /getHistory' })

    return returnResult(request, this.messageHistory)
  }

  // return messages matching set of keys
  async #getMessages(request: Request, apiBody: ChannelApiBody) {
    _sb_assert(apiBody && apiBody.apiPayload, "getMessages(): need payload (the keys)")
    if (dbg.DEBUG) console.log("==== ChannelServer.getMessages() called ====")
    try {
      if (!(apiBody.apiPayload instanceof Set)) throw new Error("[getMessages] payload needs to be a set of keys")
      const clientKeys = apiBody.apiPayload as Set<string>
      if (clientKeys.size === 0) return returnError(request, "[getMessages] No keys provided", 400)
      const regex = /^([a-zA-Z0-9]+)______([0-3]{26})$/;
      const validKeys: Array<string> = [];
      
      // const ttl0bufferKeys = this.ttl0Buffer.getMessageKeys()
      // const requestedTTL0Keys = []
      for (const key of clientKeys) {
        // we confirm format of the key
        if (regex.test(key)) {
          // if (ttl0bufferKeys.has(key)) {
          //   requestedTTL0Keys.push(key)
          // } else {
            validKeys.push(key);
          // }
        } else if (dbg.DEBUG) {
          console.error("ERROR: tried to read key with invalid key format: ", key)
        }
      }

      if (validKeys.length === 0 /* && requestedTTL0Keys.length === 0 */) {
        if (dbg.DEBUG) {
          console.log('\n', SEP, "And here are the keys that were requested", "\n", SEP)
          console.log(apiBody.apiPayload)
          console.log(clientKeys)
          console.log('\n', SEP, "==== ChannelServer.getMessages() returning ====\n", "No valid keys found", "\n", SEP)
          // console.log(this.messageKeysCacheMap)
        }
        return returnError(request, "[getMessages] No valid keys found", 400)
      }
      const getOptions: DurableObjectGetOptions = { allowConcurrency: true };
      let messages: Map<string, ArrayBuffer> = new Map();
      if (validKeys.length > 0) messages = await this.storage.get(validKeys, getOptions); // get stored messages
      // if (requestedTTL0Keys.length > 0) messages = new Map([...messages, ...this.ttl0Buffer.getMessages(requestedTTL0Keys)])
      _sb_assert(messages, "Internal Error [L1768]");
      // if (dbg.DEBUG) console.log(SEP, "==== ChannelServer.getMessages() returning ====\n", messages, "\n", SEP)
      return returnResult(request, messages);
    } catch (error: any) {
      return returnError(request, error.message, 400);
    }
  }

  // clientKeys: Set<string>): Promise<Map<string, unknown>> {

  //   // Filter the client-provided keys against the messageKeysCacheMap
  //   const validKeys = [...clientKeys].filter(key => this.messageKeysCacheMap.has(key));

  //   // Fetch the objects from storage that correspond to the valid keys
  //   const getOptions: DurableObjectGetOptions = { allowConcurrency: true };
  //   const messages = await this.storage.get(validKeys, getOptions);
  //   _sb_assert(messages, "Internal Error [L475]");

  //   // Return the fetched messages
  //   return messages;
  // }

  async #setCapacity(request: Request) {
    const { searchParams } = new URL(request.url);
    const newLimit = searchParams.get('capacity');
    this.capacity = Number(newLimit) || this.capacity;
    await this.storage.put('capacity', this.capacity)
    return returnResultJson(request, { capacity: newLimit });
  }

  async #acceptVisitor(request: Request, apiBody: ChannelApiBody) {
    _sb_assert(apiBody.apiPayload, "[acceptVisitor] need to provide userId")
    // const data = extractPayload(apiBody.apiPayload!).payload
    const data = apiBody.apiPayload
    if (data && data.userId && typeof data.userId === 'string') {
      if (!this.accepted.has(data.userId)) {
        if (this.accepted.size >= this.capacity)
          return returnError(request, `This would exceed current channel capacity (${this.capacity}); update that first`, 400)
        // add to our accepted list
        this.accepted.add(data.userId)
        // write it back to storage
        await this.storage.put('accepted', assemblePayload(this.accepted))
      }
      return returnSuccess(request);
    } else {
      return returnError(request, "[acceptVisitor] could not parse the provided userId", 400)
    }
  }

  async #lockChannel(request: Request, _apiBody: ChannelApiBody) {
    // ToDo: shut down any open websocket sessions
    this.locked = true;
    await this.storage.put('locked', this.locked)
    this.sessions.forEach((session) => { session.quit = true; });
    return returnSuccess(request);
  }

  /* NOTE: current design limits this to 2^52 bytes, future limit will be 2^64 bytes */
  #roundSize(size: number, roundUp = true) {
    if (size === Infinity) return Infinity; // special case
    if (size <= serverConstants.STORAGE_SIZE_MIN) size = serverConstants.STORAGE_SIZE_MIN;
    if (size > (2 ** 52)) throw new Error(`Storage size too large (max 2^52 and we got ${size})`);
    const exp1 = Math.floor(Math.log2(size));
    const exp2 = exp1 - 3;
    const frac = Math.floor(size / (2 ** exp2));
    const result = frac << exp2;
    if ((size > result) && roundUp) return result + (2 ** exp2);
    else return result;
  }

  // will 'spend' this amount; if there are any issues, we throw, otherwise the
  // amount actually spent is returned (which can differ from the request in
  // many ways). if 'round' is set (default), will apply rounding semantics
  // (such as for storage). otherwise it accepts any positive integer (eg set to
  // 'false' for API charges)
  async #consumeStorage(size: number, round = true): Promise<number> {
    if (dbg.DEBUG2) console.log("Consuming storage: ", size, round ? " (will round up)" : "", "current limit:", this.storageLimit, "bytes")
    if (round)
      size = this.#roundSize(size) || 0;
    size = Math.ceil(size);
    if (!size || !Number.isInteger(size) || size <= 0) {
      console.log("Problem with size:\n", size)
      if (DBG0 || dbg.LOG_ERRORS) console.trace(SEP, `[consumeStorage] invalid size:\n`, size, SEP)
      throw new SBError("'size' missing in API call or internally, or zero, or can't parse")
    }
    // get the requested amount, apply various semantics on the budd operation
    if (size >= 0) {
      if ((size === Infinity) || (size > serverConstants.MAX_BUDGET_TRANSFER)) {
        if (dbg.DEBUG2) console.log(`this value for transferBudget will be interpreted as stripping (all ${this.storageLimit} bytes):`, size)
        size = this.storageLimit; // strip it
      }
      if (size > this.storageLimit)
        // if a specific amount is requested that exceeds the budget, we do NOT interpret it as plunder
        throw new SBError(`Not enough storage budget in mother channel - requested ${size} from '${this.channelId?.slice(0,8)}...', ${this.storageLimit} available`);
    } else {
      // if it's negative, it's interpreted as how much to leave behind
      const _leaveBehind = -size;
      if (_leaveBehind > this.storageLimit)
        throw new SBError(`Not enough storage budget in mother channel - requested to leave behind ${_leaveBehind}, ${this.storageLimit} available`);
        size = this.storageLimit - _leaveBehind;
    }
    this.storageLimit -= size; // apply reduction
    this.storageLimit = Math.floor(this.storageLimit); // make sure it's an integer
    await this.storage.put('storageLimit', this.storageLimit); // here we've consumed it
    if (dbg.DEBUG2) console.log("[#consumeSTorage] Storage amount consumed:", size, "bytes; new limit:", this.storageLimit, "bytes") 
    return size // return what was actually consumed
  }

  // prints the token, doesn't validate anything
  #generateStorageToken(size: number): SBStorageToken {
    const hash = SBStorageTokenPrefix + arrayBufferToBase62(crypto.getRandomValues(new Uint8Array(32)).buffer);
    const token: SBStorageToken = {
      hash: hash,
      used: false,
      size: size, // note that this is actual size, not requested (might be the same)
      motherChannel: this.channelId!,
      created: Date.now()
    }
    return validate_SBStorageToken(token);
  }

  // channels approve storage by creating storage token out of their budget
  async #getStorageToken(request: Request, apiBody: ChannelApiBody) {
    // TODO: per-user storage boundaries (higher priority now since this will soon support Infinity)
    _sb_assert(apiBody.apiPayload, "[getStorageToken] needs parameters")
    try {
      const payloadSize = Number(apiBody.apiPayload.size);
      const size = await this.#consumeStorage(payloadSize); // will throw if there are issues, otherwise quietly return
      // the channel was good for the charge, so create the token
      const token = this.#generateStorageToken(size);
      await this.env.LEDGER_NAMESPACE.put(token.hash, JSON.stringify(validate_SBStorageToken(token)));
      if (dbg.DEBUG)
        console.log(`[newStorage()]: Created new storage token for ${size} bytes\n`,
          SEP, `hash: ${token.hash}\n`,
          SEP, 'token:\n', token, '\n',
          SEP, 'new mother storage limit:', this.storageLimit, '\n',
          SEP, 'ledger entry:', await this.env.LEDGER_NAMESPACE.get(token.hash), '\n',
          SEP, 'separate fetch:', await getServerStorageToken(token.hash, this.env), '\n',
          SEP, 'this.env:', this.env.LEDGER_NAMESPACE, '\n',
          SEP)
      return returnResult(request, token);
    } catch (error: any) {
      if (dbg.LOG_ERRORS) console.error(`[getStorageToken] ${error.message}`)
      if (error instanceof SBError) return returnError(request, '[getStorageToken] ' + error.message, 507);
      else return returnError(request, ANONYMOUS_CANNOT_CONNECT_MSG, 401);
    }
  }

  async #getStorageLimit(request: Request) {
    // ToDo: per-user storage boundaries
    return returnResultJson(request, { storageLimit: this.storageLimit });
  }

  // /*
  //   Transfer storage budget to another channel. Note that this request enters
  //   the server as 'budd': if the accessed channel does not exist, it is treated
  //   as this.#create(), otherwise it's treated as this.#deposit().
  // */
  // async #deposit(request: Request, apiBody: ChannelApiBody): Promise<Response> {
  //   _sb_assert(apiBody.apiPayload, "[budd] needs parameters")
  //   var { targetChannel, transferBudget } = apiBody.apiPayload
  //   if (!targetChannel) return returnError(request, "[budd] targetChannel missing in API call")
  //   if (!transferBudget) transferBudget = Infinity // default
  //   try {
  //     targetChannel = validate_SBChannelData(apiBody.apiPayload) // will throw if anything wrong
  //     if (!targetChannel.storageToken) return returnError(request, "[budd] storageToken missing in API call")
  //     // note that if present, it's already been validated
  //   } catch (error: any) {
  //     return returnError(request, "[budd] unable to parse channelData payload", 400);
  //   }
  //   if (dbg.DEBUG) console.log("++++ deposit() from token: channelData:\n====\n", targetChannel, "\n", "====")

  //   if (this.channelId === targetChannel) return returnResult(request, this.channelData)

  //   // performing removal of budget - first deduct then kick off creation
  //   const newStorageLimit = this.storageLimit - transferBudget;
  //   this.storageLimit = newStorageLimit;
  //   await this.storage.put('storageLimit', newStorageLimit);
  //   if (dbg.DEBUG) console.log(`[budd()]: Removed ${transferBudget} bytes from ${this.channelId!.slice(0, 12)}... and forwarding to ${targetChannel.slice(0, 12)}... (new mother storage limit: ${newStorageLimit} bytes)`);
  //   // we block on ledger since this will be verified
  //   await this.env.LEDGER_NAMESPACE.put(targetChannel, JSON.stringify({ mother: this.channelId, size: transferBudget }));
  //   if (dbg.DEBUG) console.log('++++ putting budget in ledger ... reading back:', await this.env.LEDGER_NAMESPACE.get(targetChannel))

  //   // note that if the next operation fails, the ledger entry for the transfer is still there for possible recovery
  //   return callDurableObject(targetChannel, ['uploadChannel'], newRequest, this.env)

  //   // return callDurableObject(targetChannel, [ `budd?targetChannel=${targetChannel}&transferBudget=${size}&serverSecret=${_secret}` ], request, this.env);
  // }

  #_getAdminData(): ChannelAdminData {
    const adminData: ChannelAdminData = {
      channelId: this.channelId!,
      channelData: this.channelData!,
      // joinRequests: this.joinRequests,
      capacity: this.capacity,
      locked: this.locked,
      accepted: this.accepted,
      storageLimit: this.storageLimit,
      visitors: this.visitors,
      motherChannel: this.motherChannel ?? "<UNKNOWN>",
      latestTimestamp: Channel.timestampToBase4String(this.latestTimestamp),
    }
    return adminData
  }

  async #getAdminData(request: Request) {
    try {
      const adminData = this.#_getAdminData()
      if (dbg.DEBUG) console.log("[handleAdminDataRequest] adminData:", adminData)
      return returnResult(request, adminData);
    } catch (err) {
      if (dbg.DEBUG) console.log("[#handleAdminDataRequest] Error:", err)
      throw err
    }
  }

  #registerDevice(request: Request) {
    return returnError(request, "registerDevice is disabled", 400)
  }

  // MTG ToDo: review this, it sends a return value that is not used? also doesn't use the message?
  // ToDo: how would we handle notifications sent to a specific user, eg non-broadcast messages?
  // todo: shouldn't this only be sent to users who are not connected in a session to the channel?
  async #sendWebNotifications() {
    const envNotifications = this.env.notifications
    if (!envNotifications) {
      if (dbg.DEBUG2) console.log("Cannot send web notifications (expected behavior if you're running locally")
      return;
    }
    if (dbg.DEBUG2) console.log("Sending web notification")

    // message = jsonParseWrapper(message, 'L999')
    // if (message?.type === 'ack') return

    const date = new Date();
    date.setSeconds(0);
    date.setMilliseconds(0);
    try {
      const options = {
        method: "POST",
        body: JSON.stringify({
          "channel_id": this.channelId,
          "notification": {
            silent: false,
            // replace notification in the queue, this limits message spam.
            // We are limited in how we want to deliever notifications because of the encryption of messages
            // We limit the number of notifications to 1 per minute
            tag: `${this.channelId}${date.getTime()}`,
            title: "You have a new message!",
            vibration: [100, 50, 100, 50, 350],
            // requireInteraction: true,
          }
        }),
        headers: {
          // "Content-Type": "application/json"
          "Content-Type": "application/octet-stream"
        }
      }
      return await fetch(this.webNotificationServer, options)
    } catch (err) {
      console.log(err)
      console.log("Error sending web notification")
      return err
    }
  }

  // used for 'growth' - applyng a storage token either to top up a channel's
  // budget, or to create a new channel
  async #handleBuddRequest(request: Request, apiBody: ChannelApiBody, newChannel: boolean): Promise<Response> {
    _sb_assert(apiBody.apiPayload, "[budd] needs parameters")
    var targetChannel: SBChannelData, currentStorageLimit: number = 0;
    try {
      targetChannel = validate_SBChannelData(apiBody.apiPayload) // will throw if anything wrong
      if (!targetChannel.storageToken) return returnError(request, "[budd] storageToken missing in API call")
    } catch (error: any) {
      return returnError(request, "[budd] unable to parse channelData payload", 400);
    }

    if (newChannel) {
      if (dbg.DEBUG) console.log("[budd] creating new channel\nchannelData:\n====\n", targetChannel, "\n", "====")
      // sanity check, need to verify channelData is valid (consistent)
      const targetChannelOwner = await new SB384(targetChannel.ownerPublicKey).ready
      if (targetChannelOwner.ownerChannelId !== targetChannel.channelId) {
        // this could be an error, or an attempt to hack; always log
        console.error("ERROR **** channelData ownerChannelId does not match channelId - possible hack attempt?")
        return returnError(request, ANONYMOUS_CANNOT_CONNECT_MSG, 401);
      }
    } else {
      currentStorageLimit = this.storageLimit; // it's a 'deposit' so we are adding to this
    }

    const serverToken = await getServerStorageToken(targetChannel.storageToken.hash, this.env)
    if (!serverToken) {
      if (dbg.LOG_ERRORS) {
        console.error(`ERROR **** Cannot find storage token with hash '${targetChannel.storageToken.hash}'\nTarget channel:\n`, targetChannel)
      }
      return returnError(request, ANONYMOUS_CANNOT_CONNECT_MSG, 401);
    }
    if (serverToken.used === true) {
      if (dbg.LOG_ERRORS) console.log(`ERROR **** Token already used\n`, targetChannel)
      return returnError(request, ANONYMOUS_CANNOT_CONNECT_MSG, 401);
    }
    if (dbg.DEBUG) console.log("[budd] consuming this token, ledger side version: ", serverToken)

    if (newChannel && serverToken.size! < serverConstants.NEW_CHANNEL_MINIMUM_BUDGET)
      return returnError(request, `[budd] Not enough for a new channel (minimum is ${serverConstants.NEW_CHANNEL_MINIMUM_BUDGET} bytes)`, 507);

    /*
     * Above we've parsed and confirmed everything, now we carefully spend the storage
     */
    if (newChannel) {
      // initialize new channel ('this')
      await this.storage.put('channelData', JSON.stringify(targetChannel)) // now channel exists
      if (serverToken.motherChannel) {
        await this.storage.put('motherChannel', serverToken.motherChannel); // now we've left breadcrumb
      } else {
        await this.storage.put('motherChannel', "<UNKNOWN>");
        console.error('No mother channel (should no longer happen')
      }
    }

    // consume token
    serverToken.used = true;
    if (dbg.DEBUG) console.log("[budd] using token, ledger side updated to: ", serverToken)
    await this.env.LEDGER_NAMESPACE.put(serverToken.hash, JSON.stringify(serverToken)) // now token is spent
    // right here tiny chance of any issues, might attempt a transaction in future
    const newStorageLimit = currentStorageLimit + serverToken.size!;
    await this.storage.put('storageLimit', newStorageLimit); // and now new channel can spend it
    this.storageLimit = newStorageLimit; // and now new channel can spend it
    if (dbg.DEBUG) console.log(`[budd] size at end of transaction: ${newStorageLimit}`)

    if (newChannel) {
      if (dbg.DEBUG) console.log("++++ CALLING #initialize() on new channel")
      await this
        .#initialize(targetChannel)
        .catch(err => { return returnError(request, "ERROR: failed to initialize channel: " + err.message, 400) })
      if (dbg.DEBUG) console.log("++++ CREATED channel:", this.#describe());
    } else {
      if (dbg.DEBUG) console.log("++++ NOT A NEW CHANNEL - done, target channelId was", targetChannel.channelId)
    }
    if (dbg.DEBUG) console.log("++++ RETURNING channel:", this.#describe());
    return returnResult(request, this.channelData)
  }

  async #create(request: Request, apiBody: ChannelApiBody) {
    return this.#handleBuddRequest(request, apiBody, true)
  }

  async #deposit(request: Request, apiBody: ChannelApiBody) {
    return this.#handleBuddRequest(request, apiBody, false)
  }

  async #getChannelKeys(request: Request) {
    if (!this.channelData)
      // todo: this should be checked generically?
      return returnError(request, "Channel keys ('ChannelData') not initialized");
    else
      return returnResultJson(request, this.channelData);
  }

  async #downloadChannel(request: Request, _apiBody: ChannelApiBody) {
    return returnError(request, "downloadChannel is disabled", 400)
  }

  // used to create channels (from scratch), or upload from backup, or merge
  async #uploadChannel(request: Request) {
    return returnError(request, "uploadChannel is disabled", 400)
  }
}
