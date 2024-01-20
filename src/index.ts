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

import { sbCrypto, extractPayload, assemblePayload, ChannelMessage, setDebugLevel } from 'snackabra'
import type { EnvType } from './env'
import { VERSION } from './env'
import { _sb_assert, returnResult, returnResultJson, returnError, returnSuccess, handleErrors, serverConstants, _appendBuffer } from './workers'

console.log(`\n===============\n[channelserver] Loading version ${VERSION}\n===============\n`)

// these are overriden from wrangler.toml (don't override them here)
var DEBUG = false
var DEBUG2 = false

import type { SBChannelId, ChannelAdminData, SBUserId, SBChannelData, ChannelApiBody } from 'snackabra';
import { SB384, arrayBufferToBase64, jsonParseWrapper, 
  version, validate_ChannelApiBody, validate_ChannelMessage, validate_SBChannelData } from 'snackabra';
import { SBUserPublicKey } from 'snackabra'

const SEP = '='.repeat(60) + '\n'

// used consistently with delay 50 throughout for any fail conditions to avoid providing any info
const ANONYMOUS_CANNOT_CONNECT_MSG = "No such channel, or you are not authorized."

export default {
  async fetch(request: Request, env: EnvType) {
    if (DEBUG) {
      const msg = `==== [${request.method}] Fetch called: ${request.url}`;
      console.log(
        `\n${'='.repeat(msg.length)}` +
        `\n${msg}` +
        `\n${'='.repeat(msg.length)}`
      );
      if (DEBUG2) console.log(request.headers);
    }
    return await handleErrors(request, async () => {
      if (request.method == "OPTIONS")
        return returnResult(request);
      const path = (new URL(request.url)).pathname.slice(1).split('/');
      if ((path.length >= 1) && (path[0] === 'api') && (path[1] == 'v2'))
        return handleApiRequest(path.slice(2), request, env);
      else
        return returnError(request, "Not found (must give API endpoint '/api/v2/...')", 404)
    });
  }
}

// 'path' is the request path, starting AFTER '/api/v2'
async function handleApiRequest(path: Array<string>, request: Request, env: EnvType) {
  try {
    switch (path[0]) {
      case 'info':
        return returnResultJson(request, channelServerInfo(request, env))
      case 'channel':
        if (!path[1]) throw new Error("channel needs more params")
        // todo: currently ALL api calls are routed through the DO, but there are some we could do at the microservice level
        return callDurableObject(path[1], path.slice(2), request, env);
      case "notifications":
        return returnError(request, "Device (Apple) notifications are disabled (use web notifications)", 400);
      case "getLastMessageTimes":
        // ToDo: this needs to be modified to receive a userId for each channel requested
        //       as well as limit how many can be queried at once
        return returnError(request, "getLastMessageTimes disabled on this server (see release notes)", 400)
      // {
      //   const _rooms: any = await request.json();
      //   const lastMessageTimes: Array<any> = [];
      //   for (let i = 0; i < _rooms.length; i++) {
      //     lastMessageTimes[_rooms[i]] = await lastTimeStamp(_rooms[i], env);
      //   }
      //   return returnResult(request, lastMessageTimes, 200);
      // }
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
function channelServerInfo(request: Request, env: EnvType) {
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
    version: VERSION,
    storageServer: storageUrl,
    jslibVersion: version
  }
  return retVal
}

type ApiCallMap = {
  [key: string]: ((arg0: Request, arg1: ChannelApiBody) => Promise<Response>) | undefined;
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

/**
 *
 * ChannelServer Durable Object Class
 * 
 * One instance per channel/room.
 */
export class ChannelServer implements DurableObject {
  channelId?: SBChannelId; // convenience copy of what's in channeldata
  /* all these properties are backed in storage (both global and DO KV) with below keys */
  /* ----- these do not change after creation ----- */
  /* 'channelData'         */ channelData?: SBChannelData
  /* 'motherChannel'       */ motherChannel?: SBChannelId
  /* ----- these are updated dynamically      ----- */
  /* 'storageLimit'        */ storageLimit: number = 0;
  /* 'channelCapacity'     */ channelCapacity: number = 20;
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
    s += `channelCapacity: ${this.channelCapacity}\n`;
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
      // there's a 'hidden' "/create" endpoint, see below
      "/downloadData": this.#downloadAllData.bind(this),
      "/getChannelKeys": this.#getChannelKeys.bind(this),
      "/getMessageKeys": this.#getMessageKeys.bind(this),
      "/getMessages": this.#getMessages.bind(this),
      "/getStorageLimit": this.#getStorageLimit.bind(this), // ToDo: should be per-userid basis
      "/oldMessages": this.#handleOldMessages.bind(this),
      "/registerDevice": this.#registerDevice.bind(this), // deprecated/notfunctional
      "/send": this.#handleSend.bind(this),
      "/storageRequest": this.#handleNewStorage.bind(this),
      "/uploadChannel": this.#uploadData.bind(this),
    }
    this.ownerCalls = {
      "/acceptVisitor": this.#acceptVisitor.bind(this),
      "/budd": this.#handleBuddRequest.bind(this),
      "/getAdminData": this.#handleAdminDataRequest.bind(this),
      "/lockChannel": this.#lockChannel.bind(this),
      "/updateChannelCapacity": this.#handleChannelCapacityChange.bind(this),
    }
  }

  // load channel from storage: either it's been descheduled, or it's a new channel (that has already been created)
  async #initialize(channelData: SBChannelData) {
    try {
      _sb_assert(channelData && channelData.channelId, "ERROR: no channel data found in parameters (fatal)")
      if (DEBUG) console.log(`==== ChannelServer.initialize() called for channel: ${channelData.channelId} ====`)

      const channelState = await this.storage.get(['channelId', 'channelData', 'motherChannel', 'storageLimit', 'channelCapacity', 'lastTimestamp', 'visitors', 'locked'])
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
      this.channelCapacity = Number(channelState.get('channelCapacity')) || 20
      this.lastTimestamp = Number(channelState.get('lastTimestamp')) || 0;
      this.locked = (channelState.get('locked')) === 'true' ? true : false;

      // we do these LAST since it signals that we've fully initialized the channel
      this.channelId = channelData.channelId;
      this.channelData = channelData

      // we prefetch message keys; 1000 is max.  we only prefetch/cache non-subchannel messages for now
      const listOptions: DurableObjectListOptions = { limit: 1000, prefix: this.channelId! + '______', reverse: true };
      const keys = Array.from((await this.storage.list(listOptions)).keys());
      if (keys) this.messageKeysCache = keys // else leave it empty
      this.lastCacheTimestamp = this.lastTimestamp; // todo: add a check that ts of last key is same
      this.messageKeysCacheMap = new Map(keys.map((key, index) => [key, index]));

      if (DEBUG) console.log("++++ Done initializing channel:\n", this.channelId, '\n', this.channelData)
      if (DEBUG2) console.log(SEP, 'Full DO info\n', this, '\n', SEP)
    } catch (error: any) {
      const msg = `ERROR failed to initialize channel [L250]: ${error.message}`
      if (DEBUG) console.error(msg)
      throw new Error(msg)
    }
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
      const requestClone = request.clone(); // todo: might not be needed to be fully cloned here
      var _apiBody: ChannelApiBody
      const contentType = request.headers.get('content-type')
      if (!(request.method === 'POST' && contentType && request.body)) {
        if (DEBUG) {
          console.log("---- fetch() called, but not 'POST' and/or no body")
          console.log(request.method)
          console.log(contentType)
          console.log(request.body)
        }
        return returnError(request, "Channel API call yet no body content or malformed body", 400)
      }

      // we accept json or binary (but soon only binary)
      if (contentType.indexOf("application/json") !== -1) {
        _apiBody = jsonParseWrapper(await request.json(), "L289");
        console.warn("WARNING: using JSON for channel API call, soon to be deprecated (use binary)")
      } else if (contentType.indexOf("application/octet-stream") !== -1) {
        const ab = await request.arrayBuffer()
        _apiBody = extractPayload(ab).payload
      } else {
        return returnError(request, `Channel API call but do not understand content type ${contentType}`, 400)
      }
      const apiBody = validate_ChannelApiBody(_apiBody) // will throw if anything wrong

      // if there's an apiPayloadBuf, we need to extract it
      if (apiBody.apiPayload) return returnError(request, "[fetch]: do not provide 'apiPayload'", 400)
      if (apiBody.apiPayloadBuf) {
        apiBody.apiPayload = extractPayload(apiBody.apiPayloadBuf).payload
        if (!apiBody.apiPayload) return returnError(request, "[fetch]: cannot extract from provided apiPayloadBuf", 400)
      }

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
      // phase 'two' - catch 'create' call, it's special
      
      if (this.channelId && channelId && (this.channelId !== channelId)) return returnError(request, "Internal Error (L478)");

      if (apiCall === '/create') {
        if (this.channelId) return returnError(request, `ERROR: channel already exists (asked to create '${channelId}')`, 400);
        if (DEBUG) console.log('\n', SEP, '\n', 'NEW CHANNEL ... creating ...')
        const ret = await this.#createChannel(requestClone, apiBody);
        if (DEBUG) console.log('.... created\n', SEP)
        if (ret) return ret; // if there was an error, return it, otherwise it was successful
        else return returnSuccess(request)
      }

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
        if (this.visitors.size >= this.channelCapacity) {
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

  // safety/privacy measures
  #stripChannelMessage(msg: ChannelMessage): ChannelMessage {
    const ret: ChannelMessage = {}
    if (msg.f) ret.f = msg.f; else throw new Error("ERROR: missing 'f' ('from') in message")
    if (msg.c) ret.c = msg.c; else throw new Error("ERROR: missing 'ec' ('encrypted contents') in message")
    if (msg.iv) ret.iv = msg.iv; else throw new Error("ERROR: missing 'iv' ('nonce') in message")
    if (msg.s) ret.s = msg.s; else throw new Error("ERROR: missing 's' ('signature') in message")
    if (msg.ts) ret.ts = msg.ts; else throw new Error("ERROR: missing 'ts' ('timestamp') in message")
    if (msg.ttl && msg.ttl !== 0xF) ret.ttl = msg.ttl; // optional, and we strip default
    if (msg.t) ret.t = msg.t; // optional
    if (msg.i2 && msg.i2 !== '____') ret.i2 = msg.i2; // optional, also we strip out default value
    return ret
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
  
  // process message, whether coming asynchronously from API call, or from websocket
  // throw any issues back to caller. all new messages come through here.
  async #processMessage(msg: any, apiBody: ChannelApiBody): Promise<void> {
    // var message: ChannelMessage = {}
    // if (typeof msg === "string") {
    //   message = jsonParseWrapper(msg.toString(), 'L594');
    // } else if (msg instanceof ArrayBuffer) {
    //   message = extractPayload(extractPayload(msg).payload).payload // TODO: hack
    // } else {
    //   throw new Error("Cannot parse contents type (not json nor arraybuffer)")
    // }

    const message: ChannelMessage = validate_ChannelMessage(msg) // will throw if anything wrong
    if (DEBUG) console.log(
      "------------ getting message from client ------------",
      "\n", msg, "\n", 
      message, "\n",
      "-------------------------------------------------")

    if (message.ready) {
      // the client sends a "ready" message when it can start receiving
      if (DEBUG) console.log("got 'ready' message from client")
      const session = this.sessions.get(apiBody.userId)
      if (!session) throw new Error("Internal Error [L526]")
      else session.ready = true;
      // ToDo: we used to send latest 100 right away; we don't do that anymore.
      // const latest100 = assemblePayload(await this.#getRecentMessages())!; _sb_assert(latest100, "Internal Error [L548]");
      // webSocket.send(latest100);
      return; // all good
    }

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

    // Time stamps are monotonically increasing. We enforce that they must be different.
    // Stored as a string of [0-3] to facilitate prefix searches (within 4x time ranges).
    // We append "0000" for future needs, for example if we need above 1000 messages per second.
    // Can represent epoch timestamps for the next 400+ years.
    const tsNum = Math.max(Date.now(), this.lastTimestamp + 1);
    this.lastTimestamp = tsNum;
    this.storage.put('lastTimestamp', tsNum)
    const ts = tsNum.toString(4).padStart(22, "0") + "0000" // total length 26

    // appending timestamp to channel id. this is global, unique message identifier
    const key = this.channelId + '_' + message.i2 + '_' + ts;

    // TODO: sync TTL with 'i2'
    var i2Key: string | null = null
    if (message.ttl && message.ttl > 0 && message.ttl < 0xF) {
      if (message.i2[3] === '_') {
        // if there's a TTL, you can't have a subchannel using last character, this is an error
        if (DEBUG) console.error("ERROR: subchannel cannot be used with TTL")
        throw new Error("Subchannel cannot be used with TTL. Discarding message.")
      } else {
        // ttl is a digit 1-9, we append that to the i2 centerpiece
        i2Key = this.channelId + '_' + message.i2.slice(0, 3) + message.ttl + '_' + ts;
      }
    }

    // we make sure any message has been stored properly before we broadcast it

    // strip it and package it
    const messagePayload = assemblePayload(new Map([[key, assemblePayload(this.#stripChannelMessage(message))]]))!

    // ToDo: deduct storage from channel budget for message
    //       and user budget as well. use sizeOf(messagePayload) as base (and adjust down for TTL)

    // and store it for posterity both local and global KV
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

  async #handleSend(request: Request, apiBody: ChannelApiBody) {
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
          console.error(`ERROR: failed to process message: ${error.message}`)
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

    // Older API
  // fetch most recent messages from local (worker) KV
  async #getRecentMessages(howMany: number = 100, cursor = ''): Promise<Map<string, unknown>> {
    const listOptions: DurableObjectListOptions = { limit: howMany, prefix: this.channelId!, reverse: true };
    if (cursor !== '')
      // actually this is suboptimal, but it won't matter in new design
      listOptions.end = cursor; // not '.startAfter'; fetches up to cursor

    // gets (lexicographically) latest 'howMany' keys
    const keys = Array.from((await this.storage.list(listOptions)).keys());
    _sb_assert(keys, "Internal Error [L469]")

    // we fetch all keys in one go, then fetch all contents
    // see this blog post for details on why we're setting allowConcurrency:
    // https://blog.cloudflare.com/durable-objects-easy-fast-correct-choose-three/
    const getOptions: DurableObjectGetOptions = { allowConcurrency: true };
    const messageList = await this.storage.get(keys, getOptions);
    _sb_assert(messageList, "Internal Error [L475]")
    
    // update: we now return the raw messageList, and let the caller decide what to do with it
    return messageList
  }

  // Older API
  async #handleOldMessages(request: Request) {
    const { searchParams } = new URL(request.url);
    const currentMessagesLength = Number(searchParams.get('currentMessagesLength')) || 100;
    const cursor = searchParams.get('cursor') || '';
    const messageMap = await this.#getRecentMessages(currentMessagesLength, cursor);
    // let messageArray: { [key: string]: any } = {};
    // for (let [key, value] of messageMap)
    //   messageArray[key] = value;
    return returnResult(request, messageMap);
  }

  async #getMessageKeys(request: Request) {
    // ToDo: carry forward these options from handleOldMessages()
    // const { searchParams } = new URL(request.url);
    // const currentMessagesLength = Number(searchParams.get('currentMessagesLength')) || 100;
    // const cursor = searchParams.get('cursor') || '';

    // main cache should always be in sync
    _sb_assert(this.lastCacheTimestamp === this.lastTimestamp, "Internal Error [L735]")
    if (this.lastL1CacheTimestamp !== this.lastTimestamp) {
      // L1 cache is not up-to-date, update it from the L2 cache
      this.recentKeysCache = new Set(this.messageKeysCache.slice(-100));
    }
    this.lastL1CacheTimestamp = this.lastTimestamp;
    return returnResult(request, this.recentKeysCache)
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

  async #handleChannelCapacityChange(request: Request) {
    const { searchParams } = new URL(request.url);
    const newLimit = searchParams.get('capacity');
    this.channelCapacity = Number(newLimit) || this.channelCapacity;
    this.storage.put('room_capacity', this.channelCapacity)
    return returnResultJson(request, { capacity: newLimit });
  }
  

  async #acceptVisitor(request: Request, apiBody: ChannelApiBody) {
    _sb_assert(apiBody.apiPayload, "[acceptVisitor] need to provide userId")
    // const data = extractPayload(apiBody.apiPayload!).payload
    const data = apiBody.apiPayload
    if (data && data.userId && typeof data.userId === 'string') {
      if (!this.accepted.has(data.userId)) {
        if (this.accepted.size >= this.channelCapacity)
          return returnError(request, `This would exceed current channel capacity (${this.channelCapacity}); update that first`, 400)
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
  async #handleNewStorage(request: Request) {
    // ToDo: per-user storage boundaries

    const { searchParams } = new URL(request.url);
    const size = this.#roundSize(Number(searchParams.get('size')));
    const storageLimit = this.storageLimit;
    if (size > storageLimit) return returnError(request, 'Not sufficient storage budget left in channel', 507);
    if (size > serverConstants.STORAGE_SIZE_MAX)
      return returnError(request, `Storage size too large (max ${serverConstants.STORAGE_SIZE_MAX} bytes)`, 413);

    this.storageLimit = storageLimit - size;
    this.storage.put('storageLimit', this.storageLimit); // here we've consumed it
    const token = arrayBufferToBase64(crypto.getRandomValues(new Uint8Array(48)).buffer);
    const tokenData = {
      token: token,
      used: false,
      size: size,
      motherChannel: this.channelId,
      created: Date.now()
    }
    await this.env.LEDGER_NAMESPACE.put(token, JSON.stringify(tokenData));
    if (DEBUG)
      console.log(`[newStorage()]: Created new storage token for ${size} bytes\n`,
        SEP, `token: ${token}\n`,
        SEP, 'tokenData:\n', tokenData, '\n',
        SEP, 'new mother storage limit:', this.storageLimit, '\n',
        SEP, 'ledger entry:', await this.env.LEDGER_NAMESPACE.get(token), '\n', SEP)
    return returnResultJson(request, { token: token });
  }

  async #getStorageLimit(request: Request) {
    // ToDo: per-user storage boundaries
    return returnResultJson(request, { storageLimit: this.storageLimit });
  }

  /*
     Transfer storage budget from one channel to another. Use the target
     channel's budget, and just get the channel ID from the request
     and look up and increment it's budget.
  */
  async #handleBuddRequest(request: Request): Promise<Response> {
    // TODO: refactor this, no longer uses SERVER_SECRET etc
    if (DEBUG)
      return returnError(request, "budd() needs refactoring", 400)
    if (DEBUG2) console.log(request)
    const { searchParams } = new URL(request.url);
    const targetChannel = searchParams.get('targetChannel');

    if (!targetChannel)
      return returnError(request, '[budd()]: No target channel specified', 400);
    if (this.channelId === targetChannel)
      return returnSuccess(request); // no-op
    if (!this.storageLimit) {
      if (DEBUG) console.log("storageLimit missing in mother channel (?)", this.#describe());
      return returnError(request, `[budd()]: Mother channel (${this.channelId!.slice(0, 12)}...) either does not exist, or has not been initialized, or lacks storage budget`, 400);
    }

    // get the requested amount, apply various semantics on the budd operation
    let transferBudget = this.#roundSize(Number(searchParams.get('transferBudget'))) || 0;
    if (transferBudget >= 0) {
      if ((transferBudget === Infinity) || (transferBudget > serverConstants.MAX_BUDGET_TRANSFER)) {
        if (DEBUG2) console.log(`this value for transferBudget will be interpreted as stripping (all ${this.storageLimit} bytes):`, transferBudget)
        transferBudget = this.storageLimit; // strip it
      }
      if (transferBudget > this.storageLimit)
        // if a specific amount is requested that exceeds the budget, we do NOT interpret it as plunder
        return returnError(request, `[budd()]: Not enough storage budget in mother channel - requested ${transferBudget}, ${this.storageLimit} available`, 507);
    } else {
      // if it's negative, it's interpreted as how much to leave behind
      const _leaveBehind = -transferBudget;
      if (_leaveBehind > this.storageLimit)
        return returnError(request, `[budd()]: Not enough storage budget in mother channel - requested to leave behind ${_leaveBehind}, ${this.storageLimit} available`, 507);
      transferBudget = this.storageLimit - _leaveBehind;
    }
    if (transferBudget < serverConstants.NEW_CHANNEL_MINIMUM_BUDGET)
      return returnError(request, `Not enough storage request for a new channel (requested ${transferBudget} but minimum is ${serverConstants.NEW_CHANNEL_MINIMUM_BUDGET} bytes)`, 507);

    const data = await request.arrayBuffer();
    const jsonString = new TextDecoder().decode(data);
    let jsonData = jsonString ? jsonParseWrapper(jsonString, 'L1089') : {};
    if (jsonData.hasOwnProperty("SERVER_SECRET")) return returnError(request, `[budd()]: SERVER_SECRET set? Huh?`, 403);

    // jsonData["SERVER_SECRET"] = this.env.SERVER_SECRET; // authorizing this creation/transfer; ToDo: should not propagate auth in this way
    jsonData["size"] = transferBudget;
    jsonData["motherChannel"] = this.channelId; // we leave a birth certificate behind

    const newUrl = new URL(request.url);
    newUrl.pathname = `/api/channel/${targetChannel}/uploadChannel`;
    const newRequest = new Request(newUrl.toString(), {
      method: 'POST',
      body: JSON.stringify(jsonData),
      headers: {
        'Content-Type': 'application/json'
      }
    });
    if (DEBUG) console.log("[budd()]: Converting request to upload request");
    if (DEBUG2) console.log(newRequest);

    // performing removal of budget - first deduct then kick off creation
    const newStorageLimit = this.storageLimit - transferBudget;
    this.storageLimit = newStorageLimit;
    await this.storage.put('storageLimit', newStorageLimit);
    if (DEBUG) console.log(`[budd()]: Removed ${transferBudget} bytes from ${this.channelId!.slice(0, 12)}... and forwarding to ${targetChannel.slice(0, 12)}... (new mother storage limit: ${newStorageLimit} bytes)`);
    // we block on ledger since this will be verified
    await this.env.LEDGER_NAMESPACE.put(targetChannel, JSON.stringify({ mother: this.channelId, size: transferBudget }));
    if (DEBUG) console.log('++++ putting budget in ledger ... reading back:', await this.env.LEDGER_NAMESPACE.get(targetChannel))

    // note that if the next operation fails, the ledger entry for the transfer is still there for possible recovery
    return callDurableObject(targetChannel, ['uploadChannel'], newRequest, this.env)

    // return callDurableObject(targetChannel, [ `budd?targetChannel=${targetChannel}&transferBudget=${size}&serverSecret=${_secret}` ], request, this.env);
  }

  #getAdminData(): ChannelAdminData {
    const adminData: ChannelAdminData = {
      channelId: this.channelId!,
      channelData: this.channelData!,
      // joinRequests: this.joinRequests,
      channelCapacity: this.channelCapacity,
      locked: this.locked,
      accepted: this.accepted,
      storageLimit: this.storageLimit,
      visitors: this.visitors,
      motherChannel: this.motherChannel ?? "<UNKNOWN>",
      lastTimestamp: this.lastTimestamp,
    }
    return adminData
  }

  async #handleAdminDataRequest(request: Request) {
    try {
      const adminData = this.#getAdminData()
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

  async #downloadAllData(request: Request, apiBody: ChannelApiBody) {
    const storage = await this.storage.list();
    const data: any = {
      channelId: this.channelId,
      ownerPublicKey: this.channelData!.ownerPublicKey,
    };
    storage.forEach((value, key) => {
      // this gets all messages
      // TODO: need to filter out admin data and similar and only include messages here
      data[key] = value;
    });
    if (apiBody.isOwner)
      data.adminData = this.#getAdminData();

    // return returnResult(request, data);
    if (DEBUG) {
      console.log("Not returning data .. here is what would have been returned:")
      console.log(data)
    }
    return returnError(request, "downloadAllData is disabled (see TODO in code)", 400)

    // // if (await this.#verifyAuthSign(request).catch((e) => { throw e; })) {
    //   // additional info for OWNER
    //   data.adminData = { join_requests: this.joinRequests, capacity: this.channelCapacity };
    //   data.storageLimit = this.storageLimit;
    //   // data.accepted_requests = this.acceptedRequests;
    //   // data.lockedKeys = this.lockedKeys;
    //   // data.encryptedLockedKeys = this.encryptedLockedKeys;
    //   data.motherChannel = this.motherChannel;
    //   // data.pubKeys = this.visitors;
    //   // data.roomCapacity = this.channelCapacity;
    // }

    // const dataBlob = new TextEncoder().encode(JSON.stringify(data));
    // return returnResult(request, dataBlob, 200);
  }

  async #createChannel(request: Request, apiBody: ChannelApiBody): Promise<Response | null> {
    // request cloning is done by callee
    var _cd: SBChannelData | undefined = undefined
    try {
      // var _cd: SBChannelData = extractPayload(apiBody.apiPayload!).payload
      _cd = validate_SBChannelData(apiBody.apiPayload) // will throw if anything wrong
      if (!_cd.storageToken) return returnError(request, "[createChannel] storageToken missing in API call")
    } catch (error: any) {
      return returnError(request, "[createChannel] unable to parse channelData payload", 400);
    }
    const newChannelId = _cd.channelId

    if (DEBUG) console.log("++++ createChannel() from token: channelData:\n====\n", _cd, "\n", "====")

    // note: any issues involving tokens will not be provided detailed client 'debug' info
    // todo: once this working, simplify the below
    const _storage_token_hash = await this.env.LEDGER_NAMESPACE.get(_cd.storageToken!);
    if (!_storage_token_hash) {
      if(DEBUG) console.log(`ERROR **** Token '${_cd.storageToken}' not found`)
      return returnError(request, ANONYMOUS_CANNOT_CONNECT_MSG, 401);
    }
    const _ledger_resp = _storage_token_hash ? jsonParseWrapper(_storage_token_hash, 'L1307') : null;
    if (!_ledger_resp) {
      console.error(`ERROR **** Having issues processing storage token '${_cd.storageToken}'\n`, _storage_token_hash)
      return returnError(request, ANONYMOUS_CANNOT_CONNECT_MSG, 401);
    }
    if (_ledger_resp.used) {
      if (DEBUG) console.log(`ERROR **** Token '${_cd.storageToken}' already used\n`, _ledger_resp)
      return returnError(request, ANONYMOUS_CANNOT_CONNECT_MSG, 401);
    }
    if (DEBUG) console.log("++++ createChannel() from token: ledger response: ", _ledger_resp)

    const owner = new SB384(_cd.ownerPublicKey)
    await owner.ready
    const _ownerPublicKey = owner.userPublicKey
    _sb_assert(_ownerPublicKey, "ERROR: cannot ingest / process owner public key");
    if (owner.ownerChannelId !== newChannelId)
      return returnError(request, "Owner key does not match channel id", 400);

    const storageTokenSize = _ledger_resp.size
    if (storageTokenSize < serverConstants.NEW_CHANNEL_MINIMUM_BUDGET)
      return returnError(request, `Not enough for a new channel (minimum is ${serverConstants.NEW_CHANNEL_MINIMUM_BUDGET} bytes)`, 507);

    /*
     * Above we've parsed and confirmed everything, now we carefully spend
     * the storage token and initialize the channel. We do this in a way
     * that we can recover from (most) failures.
     */
    await this.storage.put('channelData', JSON.stringify(_cd)) // now channel exists
    if (DEBUG) console.log("++++ createChannel() from token, resulting channelData:\n====\n", _cd, "\n", "====")
    if (_ledger_resp.motherChannel) {
      await this.storage.put('motherChannel', _ledger_resp.motherChannel); // now we've left breadcrumb
    } else {
      await this.storage.put('motherChannel', "<UNKNOWN>");
      console.warn('No mother channel')
    }
    _ledger_resp.used = true;
    await this.env.LEDGER_NAMESPACE.put(_cd.storageToken!, JSON.stringify(_ledger_resp)) // now token is spent
    if (DEBUG) console.log(`++++ createChannel() from token: ledger entry (${_cd.storageToken}) updated to:\n`, _ledger_resp)
    await this.storage.put('storageLimit', storageTokenSize); // and now new channel can spend it
    if (DEBUG) console.log("++++ createChannel() from token: starting size: ", storageTokenSize)

    await this
      .#initialize(_cd)
      .catch(err => { return returnError(request, `Error initializing room [L1385]: ${err}`) });
    if (DEBUG) console.log("++++ CREATED channel:", this.#describe());
    return null; // null means no errors
  }

  async #getChannelKeys(request: Request) {
    if (!this.channelData)
      // todo: this should be checked generically?
      return returnError(request, "Channel keys ('ChannelData') not initialized");
    else
      return returnResultJson(request, this.channelData);
  }

  // used to create channels (from scratch), or upload from backup, or merge
  async #uploadData(request: Request) {
    if (DEBUG) console.log("==== uploadData() ====");
    if (DEBUG) console.log(`current storage limit: ${this.storageLimit} bytes`)

    // if (!this.#channelKeys)
    //   return returnError(request, "UploadData but not initialized / created", 400);    
    // const _secret = this.env.SERVER_SECRET;

    const data = await request.arrayBuffer();
    const jsonString = new TextDecoder().decode(data);
    const jsonData = jsonParseWrapper(jsonString, 'L1416');
    if (DEBUG) {
      console.log("---- uploadData(): jsonData:")
      console.log(jsonData)
      console.log('----------------------------')
    }

    // admin/superuser, eventually this is only dev/local never production
    // const requestAuthorized = jsonData.hasOwnProperty("SERVER_SECRET") && jsonData["SERVER_SECRET"] === _secret;
    const storageTokenFunded = (jsonData.hasOwnProperty("storageToken") && this.storageLimit > 0) ? true : false;

    // if (!requestAuthorized) {
    //   // if not admin, we could be owner, but for now we consume any budget
    //   const _budget_token_string = jsonData["storageToken"];
    //   if (_budget_token_string) {
    //     const _budget_token = JSON.parse(_budget_token_string);
    //     if (DEBUG) console.log("uploadData(): using budget token: ", _budget_token)
    //     const _storage_token_hash = await this.env.LEDGER_NAMESPACE.get(_budget_token.token_hash);
    //     if (!_storage_token_hash) returnError(request, "Invalid budget token", 507);
    //     const _ledger_resp = jsonParseWrapper(_storage_token_hash, 'L1329');
    //     if (DEBUG) console.log("uploadData(): _ledger_resp: ", _ledger_resp)
    //     if (_ledger_resp.used) returnError(request, "Budget token already used", 507);
    //     // spend it
    //     _ledger_resp.used = true;
    //     // we're not too picky on double-using in this case
    //     await this.env.LEDGER_NAMESPACE.put(_ledger_resp.token_hash, JSON.stringify(_ledger_resp))
    //     const newOrAddedSize = Number(_ledger_resp.size);
    //     // we check minimum levels later
    //     this.storageLimit += newOrAddedSize;
    //     await this.storage.put("storageLimit", this.storageLimit);
    //     if (DEBUG) {
    //       console.log("uploadData(): newOrAddedSize: ", newOrAddedSize)
    //       console.log("uploadData(): storageLimit: ", this.storageLimit)
    //     }
    //   }
    // }

    const { searchParams } = new URL(request.url);
    const targetChannel = searchParams.get('targetChannel');

    if (/* (requestAuthorized) && */ (jsonData.hasOwnProperty("size")) && (targetChannel === this.channelId)) {
      // we take our cue from size, see handleBuddRequest
      const size = Number(jsonData["size"]);
      _sb_assert(this.storageLimit !== undefined, "storageLimit undefined");
      const currentStorage = Number(await this.storage.get("storageLimit"));
      _sb_assert(currentStorage === this.storageLimit, "storage out of whack");
      this.storageLimit += size;
      await this.storage.put("storageLimit", this.storageLimit);
      if (DEBUG) console.log(`uploadData(): increased budget by ${this.storageLimit} bytes`)
    }

    // double check minimum budget
    if (this.storageLimit < serverConstants.NEW_CHANNEL_MINIMUM_BUDGET)
      return returnError(request, `Channel is left below minimum (minimum is ${serverConstants.NEW_CHANNEL_MINIMUM_BUDGET} bytes)`, 507);

    if ((this.channelData!.ownerPublicKey === jsonData["roomOwner"]) || /* requestAuthorized || */ storageTokenFunded) {
      if (DEBUG) console.log("==== uploadData() allowed - creating a new channel ====")

      let entriesBuffer: Record<string, string> = {};
      let i = 0;
      for (const key in jsonData) {
        //
        // TODO:  we want a regex for this, we use it for download and other purposes as well
        // we only allow imports here of keys that correspond to messages.
        // in the json they'll look something like: ToDO: no they don't anymore, update this.
        //
        // "hvJQMhmhaIQy...ttsu5G6P0110000101110100...110010011": "{\"encrypted_contents\":{\"content\":
        // \"rZU2T5AYYFwQwHqW0AHW... very long ... zt58AF5MmEv_vLv1jGkU09\",\"iv\":\"IXsC20rryaWx9vU6\"}}",
        //
        if (key.length != 106) {
          if (DEBUG) console.log("uploadData() key skipped on 'upload': ", key)
        } else if (key.slice(0, 64) === this.channelId) {
          // the next 42 characters must be combinations of 0 and 1
          const _key = key.slice(64, 106);
          if (_key.match(/^[01]+$/)) {
            // we have a valid key, so we'll store it
            const newData = jsonData[key];
            if (newData) {
              // TODO: deduct from budget
              // we buffer writes to local object
              entriesBuffer[key] = newData;
              // but we can't do that with global
              this.env.MESSAGES_NAMESPACE.put(key, newData);
              i += 1;
              if (i > 100) {
                this.storage.put(entriesBuffer);
                entriesBuffer = {};
                i = 0;
              }
            }
          } else {
            if (DEBUG) console.log("uploadData() key not allowed (timestamp not valid): ", key)
          }
        } else {
          if (DEBUG) console.log("uploadData() key not allowed (channel id not valid): ", key)
        }
      }
      // we need to store the last batch
      if (i > 0) {
        this.storage.put(entriesBuffer);
      }
      return returnSuccess(request);
    } else {
      if (DEBUG) console.log("uploadData() not allowed (room might be partially created)")
      return returnError(request, ANONYMOUS_CANNOT_CONNECT_MSG, 401);
    }
  }
}
