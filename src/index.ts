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

import { sbCrypto, extractPayload, assemblePayload, ChannelMessage, stripChannelMessage, setDebugLevel, composeMessageKey, SBStorageToken, validate_SBStorageToken, SBStorageTokenPrefix } from 'snackabra'

import type { EnvType } from './env'

import { _sb_assert, returnResult, returnResultJson, returnError, returnSuccess, serverConstants, serverApiCosts, _appendBuffer } from './workers'
import { processApiBody } from './workers'
import { getServerStorageToken, ANONYMOUS_CANNOT_CONNECT_MSG } from './workers' 

// leave these 'false', turn on debugging in the toml file if needed
let DEBUG = false
let DEBUG2 = false

import type { SBChannelId, ChannelAdminData, SBUserId, SBChannelData, ChannelApiBody } from 'snackabra';
import {
  SB384, arrayBufferToBase62, jsonParseWrapper,
  version, validate_ChannelMessage, validate_SBChannelData
} from 'snackabra';
import { SBUserPublicKey } from 'snackabra'

const SEP = '='.repeat(60) + '\n'

export { default } from './workers'

// 'path' is the request path, starting AFTER '/api/v2'
export async function handleApiRequest(path: Array<string>, request: Request, env: EnvType) {
  DEBUG = env.DEBUG_ON; DEBUG2 = env.VERBOSE_ON;
  try {
    switch (path[0]) {
      case 'info':
        return returnResultJson(request, serverInfo(request, env))
      case 'channel':
        if (!path[1]) throw new Error("channel needs more params")
        // todo: currently ALL api calls are routed through the DO, but there are some we could do at the microservice level
        // TODO: actually we can move apiBody initial processing to here, and pass it to the DO;
        //       that allows us to do some processing here (eg. verify signature) and not in the DO.
        return callDurableObject(path[1], path.slice(2), request, env);
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
  DEBUG = env.DEBUG_ON; DEBUG2 = env.VERBOSE_ON;
  const durableObjectId = env.channels.idFromName(channelId);
  const durableObject = env.channels.get(durableObjectId);
  const newUrl = new URL(request.url);
  newUrl.pathname = "/" + channelId + "/" + path.join("/");
  const newRequest = new Request(newUrl.toString(), request);
  if (DEBUG) {
    console.log(
      "==== callDurableObject():\n",
      "channelId:", channelId, "\n",
      "path:", path, '\n',
      durableObjectId, '\n')
    // "newUrl:\n", newUrl, SEP) // very wordy
    if (DEBUG2) { console.log(request); console.log(env) }
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
    const storageServer = env.STORAGE_SERVER; // Replace with your environment variable
    const domainParts = url.hostname.split('.');
    if (storageServer && domainParts.length >= 2) {
      domainParts[0] = storageServer; // Replace the top-level domain with STORAGE_SERVER
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
    apiEndpoints: {
      visitor: loadedVisitorApiEndpoints,
      owner: loadedOwnerApiEndpoints
    },
  }
  return retVal
}

type ApiCallMap = {
  [key: string]: ((request: Request, apiBody: ChannelApiBody) => Promise<Response>) | undefined;
};

type SessionType = {
  userId: SBUserId,
  userKeys: SB384,
  channelId: SBChannelId,
  webSocket: WebSocket,
  ready: boolean,
  // blockedMessages: Map<string, unknown>,
  quit: boolean,
  receivedUserInfo: boolean
}

var loadedVisitorApiEndpoints: Array<string> = []
var loadedOwnerApiEndpoints: Array<string> = []

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
  /* 'storageLimit'        */ storageLimit: number = 0;
  /* 'capacity'            */ capacity: number = 20;
  /* 'lastTimestamp'       */ lastTimestamp: number = 0;       // monotonically increasing timestamp
  /* ----- these track access permissions      ----- */
  /* 'locked'              */ locked: boolean = false;
  /* 'visitors'            */ visitors: Map<SBUserId, SBUserPublicKey> = new Map();
  /* 'accepted'            */ accepted: Set<SBUserId> = new Set();

  /* the rest are for run time and are not backed up as such to KVs  */
  visitorKeys: Map<SBUserId, SB384> = new Map();    // convenience caching of SB384 objects for any visitors
  sessions: Map<SBUserId, SessionType> = new Map(); // track open (websocket) sessions, keyed by userId
  storage: DurableObjectStorage; // this is the DO storage, not the global KV storage
  visitorCalls: ApiCallMap; // API endpoints visitors are allowed to use
  ownerCalls: ApiCallMap;   // API endpoints that require ownership
  webNotificationServer: string;

  // used for caching message keys
  private messageKeysCache: string[] = []; // L2 cache
  messageKeysCacheMap: Map<string, number> = new Map(); // 'indexes' into messageKeysCache
  private lastCacheTimestamp: number = 0; // mirrors lastTimestamp (should be the same)
  private recentKeysCache: Set<string> = new Set(); // L1 cache
  private lastL1CacheTimestamp: number = 0; // similar but tracks the 'set'

  // DEBUG helper function to produce a string explainer of the current state
  #describe(): string {
    let s = 'CHANNEL STATE:\n';
    s += `channelId: ${this.channelData?.channelId}\n`;
    s += `channelData: ${this.channelData}\n`;
    s += `locked: ${this.locked}\n`;
    s += `capacity: ${this.capacity}\n`;
    s += `storageLimit: ${this.storageLimit}\n`;
    return s;
  }

  constructor(state: DurableObjectState, public env: EnvType) {
    // load from wrangler.toml (don't override here or in env.ts)
    DEBUG = env.DEBUG_ON
    DEBUG2 = env.VERBOSE_ON
    if (DEBUG) console.log("++++ channel server code loaded ++++ DEBUG is enabled ++++")
    if (DEBUG2) console.log("++++ DEBUG2 (verbose) enabled ++++")
    // if we're on verbose mode, then we poke jslib to be on basic debug mode
    if (DEBUG2) setDebugLevel(DEBUG)

    // durObj storage has a different API than global KV, see:
    // https://developers.cloudflare.com/workers/runtime-apis/durable-objects/#transactional-storage-api
    this.storage = state.storage; // per-DO (eg per channel) storage
    this.webNotificationServer = env.WEB_NOTIFICATION_SERVER
    this.visitorCalls = {
      "/downloadChannel": this.#downloadChannel.bind(this),
      "/getChannelKeys": this.#getChannelKeys.bind(this),
      "/getMessageKeys": this.#getMessageKeys.bind(this),
      "/getMessages": this.#getMessages.bind(this),
      "/getPubKeys": this.#getPubKeys.bind(this),
      "/getStorageLimit": this.#getStorageLimit.bind(this), // ToDo: should be per-userid basis
      "/getStorageToken": this.#getStorageToken.bind(this),
      "/registerDevice": this.#registerDevice.bind(this), // deprecated/notfunctional
      "/send": this.#send.bind(this),
      "/uploadChannel": this.#uploadChannel.bind(this),
    }
    this.ownerCalls = {
      "/acceptVisitor": this.#acceptVisitor.bind(this),
      "/budd": this.#deposit.bind(this),
      "/getAdminData": this.#getAdminData.bind(this),
      "/lockChannel": this.#lockChannel.bind(this),
      "/setCapacity": this.#setCapacity.bind(this),
    }
    loadedVisitorApiEndpoints = Object.keys(this.visitorCalls)
    loadedOwnerApiEndpoints = Object.keys(this.ownerCalls)
  }

  // load channel from storage: either it's been descheduled, or it's a new channel (that has already been created)
  async #initialize(channelData: SBChannelData) {
    try {
      _sb_assert(channelData && channelData.channelId, "ERROR: no channel data found in parameters (fatal)")
      if (DEBUG) console.log(`==== ChannelServer.initialize() called for channel: ${channelData.channelId} ====`)
      if (DEBUG) console.log("\n", channelData, "\n", SEP)

      const channelState = await this.storage.get(['channelId', 'channelData', 'motherChannel', 'storageLimit', 'capacity', 'lastTimestamp', 'visitors', 'locked'])
      const storedChannelData = jsonParseWrapper(channelState.get('channelData') as string, 'L234') as SBChannelData

      if (!channelState || !storedChannelData || !storedChannelData.channelId || storedChannelData.channelId !== channelData.channelId) {
        if (DEBUG) {
          console.log('ERROR: data missing or not matching:')
          console.log('channelState:\n', channelState)
          console.log('storedChannelData:\n', storedChannelData)
          console.log('channelData:\n', channelData)
        }
        throw new Error('Internal Error [L236]')
      }

      this.storageLimit = Number(channelState.get('storageLimit')) || 0
      this.capacity = Number(channelState.get('capacity')) || 20
      this.lastTimestamp = Number(channelState.get('lastTimestamp')) || 0;
      this.locked = (channelState.get('locked')) === 'true' ? true : false;

      // we do these LAST since it signals that we've fully initialized the channel
      this.channelId = channelData.channelId;
      this.channelData = channelData

      this.refreshCache() // see below

      if (DEBUG) console.log("++++ Done initializing channel:\n", this.channelId, '\n', this.channelData)
      if (DEBUG2) console.log(SEP, 'Full DO info\n', this, '\n', SEP)
    } catch (error: any) {
      const msg = `ERROR failed to initialize channel [L250]: ${error.message}`
      if (DEBUG) console.error(msg)
      throw new Error(msg)
    }
  }

  async refreshCache() {
    // we prefetch message keys; 1000 is max.  we only prefetch/cache non-subchannel messages for now
    const listOptions: DurableObjectListOptions = { limit: 1000, prefix: this.channelId! + '______', reverse: true };
    const keys = Array.from((await this.storage.list(listOptions)).keys());
    if (keys) this.messageKeysCache = keys // else leave it empty
    this.lastCacheTimestamp = this.lastTimestamp; // todo: add a check that ts of last key is same
    this.messageKeysCacheMap = new Map(keys.map((key, index) => [key, index]));
  }

  // this is the fetch picked up by the Durable Object
  async fetch(request: Request) {
    const url = new URL(request.url);
    const path = url.pathname.slice(1).split('/');
    // sanity check, this should always be the channel ID
    if (!path || path.length < 2)
      return returnError(request, "ERROR: invalid API (should be '/api/v2/channel/<channelId>/<api>')", 400);
    const channelId = path[0]
    const apiCall = '/' + path[1]
    try {

      if (DEBUG) console.log("111111 ==== ChannelServer.fetch() ==== phase ONE ==== 111111")
      // phase 'one' - sort out parameters

      // const requestClone = request.clone(); // appears to no longer be needed
      const requestClone = request;

      const apiBody = await processApiBody(requestClone) as Response | ChannelApiBody
      if (apiBody instanceof Response) return apiBody

      if (DEBUG) {
        console.log(
          SEP,
          '[Durable Object] fetch() called:\n',
          '  channelId:', channelId, '\n',
          '    apiCall:', apiCall, '\n',
          '  full path:', path, '\n',
          SEP, request.url, '\n', SEP,
          '    apiBody: \n', apiBody, '\n', SEP)
        if (DEBUG2) console.log(request.headers, '\n', SEP)
      }

      if (DEBUG) console.log("222222 ==== ChannelServer.fetch() ==== phase TWO ==== 222222")
      // phase 'two' - catch 'create' call (budding against a new channelId)

      if (apiCall === '/budd' && !this.channelId) {
        if (DEBUG) console.log('\n', SEP, '\n', 'Budding against a new channelId (eg creating/authorizing channel)\n', SEP, apiBody, '\n', SEP)
        return this.#create(requestClone, apiBody);
      }

      // if (and only if) both are in place, they must be consistent
      if (this.channelId && channelId && (this.channelId !== channelId)) return returnError(request, "Internal Error (L478)");

      if (DEBUG) console.log("333333 ==== ChannelServer.fetch() ==== phase THREE ==== 333333")
      // phase 'three' - check if 'we' just got created, in which case now we self-initialize
      // (we've been created but not initialized if there's no channelId, yet api call is not 'create')
      if (!this.channelId) {
        if (DEBUG) console.log("**** channel object not initialized ...")
        const channelData = jsonParseWrapper(await (this.storage.get('channelData') as Promise<string>), 'L495') as SBChannelData
        if (!channelData || !channelData.channelId) {
          // no channel, no object, no upload, no dice
          if (DEBUG) console.error('Not initialized, but channelData is not in KV (?). Here is what we know:\n', channelId, '\n', channelData, '\n', SEP, '\n', this.#describe(), '\n', SEP)
          return returnError(request, ANONYMOUS_CANNOT_CONNECT_MSG, 401);
        }
        // channel exists but object needs reloading
        if (channelId !== channelData.channelId) {
          if (DEBUG) console.log("**** channelId mismatch:\n", channelId, "\n", channelData);
          return returnError(request, "Internal Error (L327)");
        }
        // bootstrap from storage
        await this
          .#initialize(channelData) // it will throw an error if there's an issue
          .catch(() => { return returnError(request, `Internal Error (L332)`); });
      }

      if (DEBUG) console.log("444444 ==== ChannelServer.fetch() ==== phase FOUR ==== 444444")
      // phase 'four' - if we're locked, then only accepted visitors can do anything at all

      if (this.channelId !== path[0])
        return returnError(request, "ERROR: channelId mismatch (?) [L454]");

      // if we're locked, and this is not an owner call, then we need to check if the visitor is accepted
      if (this.locked && !apiBody.isOwner && !this.accepted.has(apiBody.userId))
        return returnError(request, ANONYMOUS_CANNOT_CONNECT_MSG, 401);

      // and if it's not locked, then we keep track of visitors, up to capacity level
      if (!this.locked && !this.visitors.has(apiBody.userId)) {
        // new visitor
        if (!apiBody.userPublicKey)
          return returnError(request, "Need your userPublicKey on this (or prior) operation/message ...", 401);
        if (this.visitors.size >= this.capacity) {
          if (DEBUG) console.log(`---- channel ${this.channelId} full, rejecting new visitor ${apiBody.userId}`)
          return returnError(request, ANONYMOUS_CANNOT_CONNECT_MSG, 401);
        }
        this.visitors.set(apiBody.userId, apiBody.userPublicKey)
        await this.storage.put('visitors', assemblePayload(this.visitors))
        this.visitorKeys.set(apiBody.userId, await (new SB384(apiBody.userPublicKey).ready))
      }

      if (DEBUG) console.log("555555 ==== ChannelServer.fetch() ==== phase FIVE ==== 555555")
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
        if (DEBUG) {
          console.error("ERROR: signature verification failed")
          console.log("apiBody:\n", apiBody)
        }
        return returnError(request, ANONYMOUS_CANNOT_CONNECT_MSG, 401);
      }

      // form our own opinion if this is the Owner
      apiBody.isOwner = this.channelId === sender.ownerChannelId

      if (DEBUG) console.log("666666 ==== ChannelServer.fetch() ==== phase SIX ==== 666666")
      // phase 'six' - we're ready to process the api call!

      // ToDo: verify that the 'embeded' path is same as path coming through in request

      if (apiCall === "/websocket") {
        console.log("==== ChannelServer.fetch() ==== websocket request ====")
        if (DEBUG) console.log("---- websocket request")
        if (request.headers.get("Upgrade") != "websocket") {
          if (DEBUG) console.log("---- websocket request, but not websocket (error)")
          return returnError(request, "Expected websocket", 400);
        }
        const ip = request.headers.get("CF-Connecting-IP");
        const pair = new WebSocketPair(); // that's CF websocket pair
        if (DEBUG) console.log("---- websocket request, creating session")
        await this.#setUpNewSession(pair[1], ip, apiBody); // we use one ourselves
        if (DEBUG) console.log("---- websocket request, returning session")
        return new Response(null, { status: 101, webSocket: pair[0] }); // we return the other to the client
      } else if (this.ownerCalls[apiCall]) {
        console.log("==== ChannelServer.fetch() ==== owner call ====")
        if (!apiBody.isOwner) {
          if (DEBUG) console.log("---- owner call, but not owner (error)");
          return returnError(request, ANONYMOUS_CANNOT_CONNECT_MSG, 401);
        }
        try {
          const result = await this.ownerCalls[apiCall]!(request, apiBody);
          if (DEBUG) console.log("owner call succeeded")
          return result
        } catch (error: any) {
          console.log("ERROR: owner call failed: ", error)
          console.log(error.stack)
          return returnError(request, `API ERROR [L410] [${apiCall}]: ${error.message} \n ${error.stack}`);
        }
      } else if (this.visitorCalls[apiCall]) {
        console.log("==== ChannelServer.fetch() ==== visitor call ====")
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

  #appendMessageKeyToCache(newKey: string): void {
    if (this.messageKeysCache.length > 5000) {
      // kludgy limiting of state size
      const keys = this.messageKeysCache.slice(-500);
      // recreate the array and map
      this.messageKeysCache = keys
      this.messageKeysCacheMap = new Map(keys.map((key, index) => [key, index]));
    }
    this.messageKeysCache.push(newKey);
    this.messageKeysCacheMap.set(newKey, this.messageKeysCache.length - 1);
  }

  // all messages come through here. they're either through api call or websocket.
  // any issues will throw.
  async #processMessage(msg: any, apiBody: ChannelApiBody): Promise<void> {

    // var message: ChannelMessage = {}
    // if (typeof msg === "string") {
    //   message = jsonParseWrapper(msg.toString(), 'L594');
    // } else if (msg instanceof ArrayBuffer) {
    //   message = extractPayload(extractPayload(msg).payload).payload // TODO: hack
    // } else {
    //   throw new Error("Cannot parse contents type (not json nor arraybuffer)")
    // }

    if (DEBUG) console.log(
      "------------ getting message from client (pre validation) ------------",
      "\n", msg, "\n",
      "-------------------------------------------------")

    if (msg.ready) {
      // the client sends a "ready" message when it can start receiving; reception means websocket is all set up
      if (DEBUG) console.log("got 'ready' message from client")
      const session = this.sessions.get(apiBody.userId)
      if (!session) throw new Error("Internal Error [L526]")
      else session.ready = true;
      // ToDo: we used to send latest 100 right away; we don't do that anymore.
      // const latest100 = assemblePayload(await this.#getRecentMessages())!; _sb_assert(latest100, "Internal Error [L548]");
      // webSocket.send(latest100);
      return; // all good
    }

    const message: ChannelMessage = validate_ChannelMessage(msg) // will throw if anything wrong

    // at this point we have a validated, verified, and parsed message
    // a few minor things to check before proceeding

    if (typeof message.c === 'string') {
      const msg = "[processMessage]: Contents ('c') sent as string. Discarding message."
      if (DEBUG) console.error(msg)
      throw new Error(msg);
    }

    if (message.i2 && !apiBody.isOwner) {
      if (DEBUG) console.error("ERROR: non-owner message setting subchannel")
      throw new Error("Only Owner can set subchannel. Discarding message.");
    } else if (!message.i2) {
      message.i2 = '____' // default; use to keep track of where we're at in processing
    }

    const tsNum = Math.max(Date.now(), this.lastTimestamp + 1);
    message.sts = tsNum; // server timestamp
    this.lastTimestamp = tsNum;
    this.storage.put('lastTimestamp', tsNum)

    // appending timestamp to channel id. this is global, unique message identifier
    // const key = this.channelId + '_' + message.i2 + '_' + ts;
    const key = composeMessageKey(this.channelId!, tsNum, message.i2)

    // TODO: sync TTL with 'i2'
    var i2Key: string | null = null
    if (message.ttl && message.ttl > 0 && message.ttl < 0xF) {
      // these are the cases where messages are also stored under a TTL subchannel
      if (message.i2[3] === '_') {
        // if there's a TTL, you can't have a subchannel using last character, this is an error
        if (DEBUG) console.error("ERROR: subchannel cannot be used with TTL")
        throw new Error("Subchannel cannot be used with TTL. Discarding message.")
      } else {
        // ttl is a digit 1-9, we append that to the i2 centerpiece (eg encoded in subchannel)
        // i2Key = this.channelId + '_' + message.i2.slice(0, 3) + message.ttl + '_' + ts;
        i2Key = composeMessageKey(this.channelId!, tsNum, message.i2.slice(0, 3) + message.ttl)
      }
    }

    // messages with destinations are not allowed to have a long-term TTL
    if (!message.ttl || message.ttl > 0x8) { // max currently defined message age, 10 days
      if (message.t)
        throw new Error("ERROR: any 'to' (routed) message must have a short TTL")
    }

    // strip (minimize) it and, package it, result chunk is stand-alone except for channelId
    const messagePayload = assemblePayload(stripChannelMessage(message))!

    // final, final, we enforce storage limit
    const messagePayloadSize = messagePayload.byteLength
    if (messagePayloadSize > serverConstants.MAX_SB_BODY_SIZE) {
      if (DEBUG) console.error(`ERROR: message too large (${messagePayloadSize} bytes)`)
      throw new Error("Message too large. Discarding message.");
    }

    if (DEBUG && messagePayloadSize <= 1024)
      console.log("[info] this message could be stored as metadata (size <= 1KB)")

    // consume storage budget
    if ((messagePayloadSize * serverApiCosts.CHANNEL_STORAGE_MULTIPLIER) > this.storageLimit)
      throw new Error("Storage limit exceeded. Message cannot be stored or broadcast. Discarding message.");
    this.storageLimit -= (messagePayloadSize * serverApiCosts.CHANNEL_STORAGE_MULTIPLIER);
    await this.storage.put('storageLimit', this.storageLimit)

    // todo: add per-user budget limits; also adjust down cost for TTL

    // and store it for posterity both local and global KV, before we broadcast
    await this.storage.put(key, messagePayload); // we wait for local to succeed before global
    await this.env.MESSAGES_NAMESPACE.put(key, messagePayload); // now make sure it's in global

    if (i2Key) {
      // we don't block on these (TTLs have slightly lower expected SLA)
      this.storage.put(i2Key, messagePayload);
      this.env.MESSAGES_NAMESPACE.put(i2Key, messagePayload);
    } else {
      // and currently it's only non-subchannel messages that we cache
      this.#appendMessageKeyToCache(key);
    }

    // everything looks good. we broadcast to all connected clients (if any)
    this.#broadcast(messagePayload)
      .catch((error: any) => {
        throw new Error(`ERROR: failed to broadcast message: ${error.message}`)
      });
  }

  async #send(request: Request, apiBody: ChannelApiBody) {
    _sb_assert(apiBody && apiBody.apiPayload, "send(): need payload (the message)")
    if (DEBUG) {
      console.log("==== ChannelServer.handleSend() called ====")
      console.log(apiBody)
    }
    try {
      await this.#processMessage(apiBody.apiPayload!, apiBody)
      return returnSuccess(request)
    } catch (error: any) {
      return returnError(request, error.message, 400);
    }
  }

  async #setUpNewSession(webSocket: WebSocket, _ip: string | null, apiBody: ChannelApiBody) {
    _sb_assert(webSocket && (webSocket as any).accept, "ERROR: webSocket does not have accept() method (fatal)");
    (webSocket as any).accept(); // typing override (old issue with CF types)
    webSocket.binaryType = "arraybuffer"; // otherwise default is 'blob'

    const userKeys = this.visitorKeys.get(apiBody.userId)!
    _sb_assert(userKeys, "Internal Error [L585]")

    // Create our session and add it to the sessions list.
    const session: SessionType = {
      userId: apiBody.userId,
      userKeys: userKeys,
      channelId: apiBody.channelId,
      webSocket: webSocket,
      ready: false,
      quit: false, // tracks cleanup, true means go away
      receivedUserInfo: false,
    };

    // track active connections
    this.sessions.set(apiBody.userId, session)

    webSocket.addEventListener("message", async msg => {
      try {
        if (session.quit) {
          webSocket.close(1011, "WebSocket broken (got a quit).");
          return;
        }
        try {
          const message = extractPayload(msg.data).payload
          if (!message) throw new Error("ERROR: could not process message payload")
          await this.#processMessage(message, apiBody); // apiBody from original setup
        } catch (error: any) {
          console.error(`[channel server websocket listener] error: <<${error.message}>>`)
          // console.log(msg)
          console.log(msg.data)
          webSocket.send(JSON.stringify({ error: `ERROR: failed to process message: ${error.message}` }));
        }
      } catch (error: any) {
        // Report any exceptions directly back to the client
        const err_msg = '[handleSession()] ' + error.message + '\n' + error.stack + '\n';
        if (DEBUG2) console.log(err_msg);
        try {
          webSocket.send(JSON.stringify({ error: err_msg }));
        } catch {
          console.error(`ERROR: was unable to propagate error to client: ${err_msg}`);
        }
      }
    });

    // On "close" and "error" events, remove matching sessions
    const closeOrErrorHandler = () => {
      session.quit = true; // tells any closure to go away
      // this.sessions = this.sessions.filter(member => member !== session);
      this.sessions.delete(apiBody.userId)
    };
    webSocket.addEventListener("close", closeOrErrorHandler);
    webSocket.addEventListener("error", closeOrErrorHandler);

    // just a little ping that we're up and running
    webSocket.send(JSON.stringify({
      ready: true,
    }));
  }

  // broadcasts a message to all clients.
  async #broadcast(messagePayload: ArrayBuffer) {
    if (this.sessions.size === 0) return; // nothing to do
    // MTG ToDo: we don't send notifications for everything? for example locked-out messages?
    if (DEBUG2) console.log("calling sendWebNotifications()", messagePayload);
    await this.#sendWebNotifications(); // ping anybody subscribing to channel
    // Iterate over all the sessions sending them messages.
    this.sessions.forEach((session, _userId) => {
      if (session.ready) {
        try {
          if (DEBUG) console.log("sending message to session (user): ", session.userId)
          session.webSocket.send(messagePayload);
        } catch (err) {
          if (DEBUG) console.log("ERROR: failed to send message to session: ", session.userId)
          session.ready = false;
          session.quit = true; // should probably close it
        }
      } else {
        if (DEBUG) console.log(`session not ready, not forwarding message to ${session.userId}`);
      }
    }
    );
  }

  async #getMessageKeys(request: Request, _apiBody: ChannelApiBody) {
    // ToDo: carry forward these options from handleOldMessages()
    //       (using abiBody format)
    // const { searchParams } = new URL(request.url);
    // const currentMessagesLength = Number(searchParams.get('currentMessagesLength')) || 100;
    // const cursor = searchParams.get('cursor') || '';

    // main cache should always be in sync
    // _sb_assert(this.lastCacheTimestamp === this.lastTimestamp, "Internal Error [L735]")
    if (this.lastCacheTimestamp !== this.lastTimestamp) {
      if (DEBUG) console.log("**** message cache is out of sync, reloading it ...")
      this.refreshCache()
    }

    if (this.lastL1CacheTimestamp !== this.lastTimestamp) {
      // L1 cache is not up-to-date, update it from the L2 cache
      this.recentKeysCache = new Set(this.messageKeysCache.slice(-100));
    }
    this.lastL1CacheTimestamp = this.lastTimestamp;
    return returnResult(request, this.recentKeysCache)
  }

  // return known set of userId (hash) -> public key
  async #getPubKeys(request: Request, _apiBody: ChannelApiBody) {
    return returnResult(request, this.visitors)
  }

  // return messages matching set of keys
  async #getMessages(request: Request, apiBody: ChannelApiBody) {
    _sb_assert(apiBody && apiBody.apiPayload, "getMessages(): need payload (the keys)")
    if (DEBUG) console.log("==== ChannelServer.getMessages() called ====")
    try {
      if (!(apiBody.apiPayload instanceof Set)) throw new Error("[getMessages] payload needs to be a set of keys")
      const clientKeys = apiBody.apiPayload as Set<string>
      const validKeys = [];
      for (const key of clientKeys) {
        if (this.messageKeysCacheMap.has(key)) {
          validKeys.push(key);
        }
      }
      if (validKeys.length === 0) return returnError(request, "[getMessages] No valid keys found", 400)
      const getOptions: DurableObjectGetOptions = { allowConcurrency: true };
      const messages = await this.storage.get(validKeys, getOptions);
      _sb_assert(messages, "Internal Error [L777]");
      // if (DEBUG) console.log(SEP, "==== ChannelServer.getMessages() returning ====\n", messages, "\n", SEP)
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
    this.storage.put('capacity', this.capacity)
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

  // channels approve storage by creating storage token out of their budget
  async #getStorageToken(request: Request, apiBody: ChannelApiBody) {
    // TODO: per-user storage boundaries (higher priority now since this will soon support Infinity)
    _sb_assert(apiBody.apiPayload, "[getStorageToken] needs parameters")

    var size = this.#roundSize(Number(apiBody.apiPayload.size)) || 0;
    if (!size) return returnError(request, "[getStorageToken] size missing in API call, or zero, or can't parse")
    // get the requested amount, apply various semantics on the budd operation
    if (size >= 0) {
      if ((size === Infinity) || (size > serverConstants.MAX_BUDGET_TRANSFER)) {
        if (DEBUG2) console.log(`this value for transferBudget will be interpreted as stripping (all ${this.storageLimit} bytes):`, size)
        size = this.storageLimit; // strip it
      }
      if (size > this.storageLimit)
        // if a specific amount is requested that exceeds the budget, we do NOT interpret it as plunder
        return returnError(request, `[budd()]: Not enough storage budget in mother channel - requested ${size} from '${this.channelId?.slice(0,8)}...', ${this.storageLimit} available`, 507);
    } else {
      // if it's negative, it's interpreted as how much to leave behind
      const _leaveBehind = -size;
      if (_leaveBehind > this.storageLimit)
        return returnError(request, `[budd()]: Not enough storage budget in mother channel - requested to leave behind ${_leaveBehind}, ${this.storageLimit} available`, 507);
        size = this.storageLimit - _leaveBehind;
    }

    this.storageLimit -= size; // apply reduction
    this.storage.put('storageLimit', this.storageLimit); // here we've consumed it

    const hash = SBStorageTokenPrefix + arrayBufferToBase62(crypto.getRandomValues(new Uint8Array(32)).buffer);
    const token: SBStorageToken = {
      hash: hash,
      used: false,
      size: size, // note that this is actual size not requested (might be the same)
      motherChannel: this.channelId!,
      created: Date.now()
    }
    await this.env.LEDGER_NAMESPACE.put(token.hash, JSON.stringify(validate_SBStorageToken(token)));
    if (DEBUG)
      console.log(`[newStorage()]: Created new storage token for ${size} bytes\n`,
        SEP, `hash: ${token.hash}\n`,
        SEP, 'token:\n', token, '\n',
        SEP, 'new mother storage limit:', this.storageLimit, '\n',
        SEP, 'ledger entry:', await this.env.LEDGER_NAMESPACE.get(token.hash), '\n',
        SEP, 'separate fetch:', await getServerStorageToken(token.hash, this.env), '\n',
        SEP, 'this.env:', this.env.LEDGER_NAMESPACE, '\n',
        SEP)
    return returnResult(request, token);
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
  //   if (DEBUG) console.log("++++ deposit() from token: channelData:\n====\n", targetChannel, "\n", "====")

  //   if (this.channelId === targetChannel) return returnResult(request, this.channelData)

  //   // performing removal of budget - first deduct then kick off creation
  //   const newStorageLimit = this.storageLimit - transferBudget;
  //   this.storageLimit = newStorageLimit;
  //   await this.storage.put('storageLimit', newStorageLimit);
  //   if (DEBUG) console.log(`[budd()]: Removed ${transferBudget} bytes from ${this.channelId!.slice(0, 12)}... and forwarding to ${targetChannel.slice(0, 12)}... (new mother storage limit: ${newStorageLimit} bytes)`);
  //   // we block on ledger since this will be verified
  //   await this.env.LEDGER_NAMESPACE.put(targetChannel, JSON.stringify({ mother: this.channelId, size: transferBudget }));
  //   if (DEBUG) console.log('++++ putting budget in ledger ... reading back:', await this.env.LEDGER_NAMESPACE.get(targetChannel))

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
      lastTimestamp: this.lastTimestamp,
    }
    return adminData
  }

  async #getAdminData(request: Request) {
    try {
      const adminData = this.#_getAdminData()
      if (DEBUG) console.log("[handleAdminDataRequest] adminData:", adminData)
      return returnResult(request, adminData);
    } catch (err) {
      if (DEBUG) console.log("[#handleAdminDataRequest] Error:", err)
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
      if (DEBUG) console.log("Cannot send web notifications (expected behavior if you're running locally")
      return;
    }
    if (DEBUG) console.log("Sending web notification")

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

  async #downloadChannel(request: Request, _apiBody: ChannelApiBody) {
    return returnError(request, "downloadChannel is disabled", 400)
  }

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
      if (DEBUG) console.log("[budd] creating new channel\nchannelData:\n====\n", targetChannel, "\n", "====")
      // sanity check, need to verify channelData is valid (consistent)
      const targetChannelOwner = await new SB384(targetChannel.ownerPublicKey).ready
      if (targetChannelOwner.ownerChannelId !== targetChannel.channelId) {
        // this could be an error, or an attempt to hack
        console.error("ERROR **** channelData ownerChannelId does not match channelId - possible hack attempt?")
        return returnError(request, ANONYMOUS_CANNOT_CONNECT_MSG, 401);
      }  
    } else {
      currentStorageLimit = this.storageLimit; // it's a 'deposit' so we are adding to this
    }

    const serverToken = await getServerStorageToken(targetChannel.storageToken.hash, this.env)
    if (!serverToken) {
      console.error(`ERROR **** Having issues processing storage token '${targetChannel.storageToken}'\n`, targetChannel)
      return returnError(request, ANONYMOUS_CANNOT_CONNECT_MSG, 401);
    }
    if (serverToken.used === true) {
      if (DEBUG) console.log(`ERROR **** Token already used\n`, targetChannel)
      return returnError(request, ANONYMOUS_CANNOT_CONNECT_MSG, 401);
    }
    if (DEBUG) console.log("[budd] consuming this token, ledger side version: ", serverToken)

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
    if (DEBUG) console.log("[budd] using token, ledger side updated to: ", serverToken)
    await this.env.LEDGER_NAMESPACE.put(serverToken.hash, JSON.stringify(serverToken)) // now token is spent
    // right here tiny chance of any issues, might attempt a transaction in future
    const newStorageLimit = currentStorageLimit + serverToken.size!;
    await this.storage.put('storageLimit', newStorageLimit); // and now new channel can spend it
    this.storageLimit = newStorageLimit; // and now new channel can spend it
    if (DEBUG) console.log(`[budd] size at end of transaction: ${newStorageLimit}`)

    if (newChannel) {
      if (DEBUG) console.log("++++ CALLING #initialize() on new channel")
      await this
        .#initialize(targetChannel)
        .catch(err => { return returnError(request, "ERROR: failed to initialize channel: " + err.message, 400) })
      if (DEBUG) console.log("++++ CREATED channel:", this.#describe());
    } else {
      if (DEBUG) console.log("++++ NOT A NEW CHANNEL - done, target channelId was", targetChannel.channelId)
    }
    if (DEBUG) console.log("++++ RETURNING channel:", this.#describe());
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

  // used to create channels (from scratch), or upload from backup, or merge
  async #uploadChannel(request: Request) {
    return returnError(request, "uploadChannel is disabled", 400)
  }
}
