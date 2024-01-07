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

import { sbCrypto, extractPayload, assemblePayload, ChannelMessage } from 'snackabra'
import type { EnvType } from './env'
import { VERSION, DEBUG, DEBUG2 } from './env'
import { _sb_assert, returnResult, returnResultJson, returnError, handleErrors, serverConstants, _appendBuffer } from './workers'

console.log(`\n===============\n[channelserver] Loading version ${VERSION}\n===============\n`)

if (DEBUG) console.log("++++ channel server code loaded ++++ DEBUG is enabled ++++")
if (DEBUG2) console.log("++++ DEBUG2 (verbose) enabled ++++")

import type { SBChannelId, ChannelAdminData, SBUserId, SBChannelData, ChannelApiBody } from 'snackabra';
import { SB384, arrayBufferToBase64, jsonParseWrapper, 
  version, validate_ChannelApiBody, validate_ChannelMessage, validate_SBChannelData } from 'snackabra';
import { SBUserPublicKey } from 'snackabra'
// const sbCrypto = new SBCrypto()

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

//#region Local TYPES

type ApiCallMap = {
  [key: string]: ((arg0: Request, arg1: ChannelApiBody) => Promise<Response>) | undefined;
};

type SessionType = {
  userId: SBUserId,
  userKeys: SB384,
  channelId: SBChannelId,
  webSocket: WebSocket,
  blockedMessages: Map<string, unknown>,
  quit: boolean,
  receivedUserInfo: boolean
}
//#endregion - Local TYPES

// <newPage> 
/**
 *
 * ChannelServer Durable Object Class
 * 
 * One instance per channel/room.
 */
export class ChannelServer implements DurableObject {
  channelId?: SBChannelId;

  /* all these properties are backed in storage (DO KV) with below keys */
  /* 'channelData'         */ channelData?: SBChannelData        // does not change
  /* 'motherChannel'       */ motherChannel?: SBChannelId      // does not change

  /* 'storageLimit'        */ storageLimit: number = 0;
  /* 'channelCapacity'     */ channelCapacity: number = 20;
  /* 'lastTimestamp'       */ lastTimestamp: number = 0;       // monotonically increasing timestamp

  /* 'locked'              */ locked: boolean = false;
  /* 'visitors'            */ visitors: Map<SBUserId, SBUserPublicKey> = new Map();
  /* 'accepted'            */ accepted: Set<SBUserId> = new Set();

  storage: DurableObjectStorage;

  initializePromise: Promise<void> | null = null;
  sessions: Array<any> = [];
  
  ownerCalls: ApiCallMap;
  visitorCalls: ApiCallMap;
  adminCalls: ApiCallMap;
  messagesCursor: string | null = null; // used for 'startAfter' option

  visitorKeys: Map<SBUserId, SB384> = new Map();

  // DEBUG helper function to produce a string explainer of the current state
  #describe(): string {
    let s = 'CHANNEL STATE:\n';
    s += `channelId: ${this.channelData?.channelId}\n`;
    s += `ownerId: ${this.channelData?.ownerPublicKey}\n`;
    s += `channelCapacity: ${this.channelCapacity}\n`;
    s += `locked: ${this.locked}\n`;
    s += `storageLimit: ${this.storageLimit}\n`;
    s += `motherChannel: ${this.motherChannel}\n`;
    return s;
  }

  constructor(state: DurableObjectState, public env: EnvType) {
    // NOTE: DO storage has a different API than global KV, see:
    // https://developers.cloudflare.com/workers/runtime-apis/durable-objects/#transactional-storage-api
    this.storage = state.storage;
    // this.env = env;

    this.visitorCalls = {
      "/channelLocked": this.#isChannelLocked.bind(this),
      // "/create": this.#createNewChannel.bind(this),
      "/downloadData": this.#downloadAllData.bind(this),
      "/getChannelKeys": this.#getChannelKeys.bind(this),
      "/oldMessages": this.#handleOldMessages.bind(this),
      // "/postPubKey": this.#postPubKey.bind(this), // deprecated
      "/registerDevice": this.#registerDevice.bind(this), // deprecated
      "/getStorageLimit": this.#getStorageLimit.bind(this), // ToDo: should be per-userid basis
      "/storageRequest": this.#handleNewStorage.bind(this),
      "/uploadChannel": this.#uploadData.bind(this)
    }

    this.ownerCalls = {
      "/acceptVisitor": this.#acceptVisitor.bind(this),
      "/budd": this.#handleBuddRequest.bind(this),
      "/getAdminData": this.#handleAdminDataRequest.bind(this),
      // "/getChannelCapacity": this.#getChannelCapacity.bind(this), // ToDo: this should be owner
      // "/getJoinRequests": this.#getJoinRequests.bind(this),
      "/getMother": this.#getMother.bind(this),
        // "/getPubKeys": this.#getPubKeys.bind(this),
      "/lockChannel": this.#lockChannel.bind(this),
      // "/ownerKeyRotation": this.#ownerKeyRotation.bind(this), // deprecated
      // "/ownerUnread": this.#getOwnerUnreadMessages.bind(this),
      "/updateChannelCapacity": this.#handleChannelCapacityChange.bind(this),
    }

    this.adminCalls = {
      // "/authorizeChannel": this.#authorizeChannel.bind(this),
    }
  }

  // load channel from storage: either it's been descheduled, or it's a new channel
  async #initialize(channelData: SBChannelData) {
    try {
      _sb_assert(channelData && channelData.channelId, "ERROR: no channel data found in parameters (fatal)")
      if (DEBUG) console.log(`==== ChannelServer.initialize() called for channel: ${channelData.channelId} ====`)

      this.lastTimestamp = Number(await this.#getKey('lastTimestamp')) || 0;
      this.channelCapacity = Number(await this.#getKey('channelCapacity')) || 20
      this.visitors = jsonParseWrapper(await this.#getKey('visitors') || JSON.stringify([]), 'L430');
      this.locked = (await this.#getKey('locked')) === 'true' ? true : false;
      this.storageLimit = Number(await this.#getKey('storageLimit'))

      // we do these LAST since it signals that we've fully initialized the channel
      this.channelId = channelData.channelId;
      this.channelData = channelData

      if (DEBUG) console.log("++++ Done initializing channel:\n", this.channelId, '\n', this.channelData)
      if (DEBUG2) console.log(SEP, 'Full DO info\n', this, '\n', SEP)

    } catch (error: any) {
      if (DEBUG) console.error(`Failed to initialize channel (${error})`)
      throw new Error(`Failed to initialize channel (${error})`)
    }

  }

  // this is the fetch picked up by the Durable Object
  async fetch(request: Request) {
    return await handleErrors(request, async () => {
      const url = new URL(request.url);
      const path = url.pathname.slice(1).split('/');

      // this should always be the channel ID
      if (!path || path.length < 2) return returnError(request, "ERROR: invalid API (should be '/api/v2/channel/<channelId>/<api>')", 400);
      const channelId = path[0]
      const apiCall = '/' + path[1]

      const requestClone = request.clone()

      try {
        var _apiBody: ChannelApiBody
        if (apiCall === '/getChannelKeys') {
          if (DEBUG) console.log("---- getChannelKeys request (no apiBody)") // only channel api that doesn't require an apiBody
        } else if (!(request.method === 'POST' && request.headers.get('content-type') === 'application/octet-stream"' && request.body)) {
          if (DEBUG) {
            console.log("---- fetch() called, but not POST or no body")
            console.log(request.method)
            console.log(request.headers.get('content-type'))
            console.log(request.body)
          }
          return returnError(request, "Channel API call yet no body content or malformed body", 400)
        } else {
          const ab = await request.arrayBuffer()
          if (DEBUG) console.log("---- fetch() called, request body:\n", ab)
          _apiBody = validate_ChannelApiBody(extractPayload(ab).payload) // will throw if anything wrong
          if (DEBUG) {
            console.log(
              '\n', SEP,
              '[Durable Object] fetch() called:\n',
              '  channelId:', channelId, '\n',
              '    apiCall:', apiCall, '\n',
              '  full path:', path, '\n',
              SEP, request.url, '\n', SEP,
              'apiBody: \n', _apiBody, '\n', SEP)
            if (DEBUG2) console.log(request.headers, '\n', SEP)
          }
        }
        const apiBody = _apiBody!
  
        if (this.channelId && channelId && (this.channelId !== channelId)) return returnError(request, "Internal Error (L478)", 500);
  
        if (apiCall === '/create') {
          if (this.channelId) return returnError(request, `ERROR: channel already exists (asked to create '${channelId}')`, 400);
          if (DEBUG) console.log('\n', SEP, '\n', 'NEW CHANNEL ... creating ...')
          const ret = await this.#createChannel(requestClone, apiBody);
          if (DEBUG) console.log('.... created\n', SEP)
          if (ret) return ret; // if there was an error, return it, otherwise it was successful
          return returnResultJson(request, { success: true }, 200);
        }
  
        // if this object doesn't have it's correct channelId, that means it needs to be initialized
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
            return returnError(request, "Internal Error (L504)", 500);
          }
          // bootstrap from storage
          await this
            .#initialize(channelData) // it will throw an error if there's an issue
            .catch(() => { return returnError(request, `Internal Error (L509)`, 500); });
        }
  
        if (this.channelId !== path[0])
          return returnError(request, "ERROR: channelId mismatch (?) [L454]", 500);

        // if we're locked, and this is not an owner call, then we need to check if the visitor is accepted
        if (this.locked && !apiBody.isOwner && !this.accepted.has(apiBody.userId))
          return returnError(request, ANONYMOUS_CANNOT_CONNECT_MSG, 401);

        // if it's not locked, we keep track of visitors
        if (!this.locked && !this.visitors.has(apiBody.userId)) {
          // new visitor
          if (!apiBody.userPublicKey)
            return returnError(request, "Need your userPublicKey on this operation/message ...", 401);
          if (this.visitors.size >= this.channelCapacity) {
            if (DEBUG) console.log(`---- channel ${this.channelId} full, rejecting new visitor ${apiBody.userId}`)
            return returnError(request, ANONYMOUS_CANNOT_CONNECT_MSG, 401);
          }
          this.visitors.set(apiBody.userId, apiBody.userPublicKey)
          await this.storage.put('visitors', assemblePayload(this.visitors))
          this.visitorKeys.set(apiBody.userId, await (new SB384(apiBody.userPublicKey).ready))
        }

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
        const apiPayloadBuf = apiBody.apiPayload
        // verification covers timestamp + path + apiPayload
        const verified = await sbCrypto.verify(sender.publicKey, apiBody.sign, apiPayloadBuf ? _appendBuffer(prefixBuf, apiPayloadBuf) : prefixBuf)
        if (!verified) {
          if (DEBUG) {
            console.error("ERROR: signature verification failed")
            console.log("apiBody:\n", apiBody)
          }
          return returnError(request, ANONYMOUS_CANNOT_CONNECT_MSG, 401);
        }
        apiBody.isOwner = this.channelId === sender.ownerChannelId

        // ToDo: verify that the 'embeded' path is same as path coming through in request
      
        if (apiCall === "/websocket") {
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
            return returnError(request, `API ERROR [${apiCall}]: ${error.message} \n ${error.stack}`, 500);
          }

          // if (await this.#verifyAuthSign(request)) {
          //   if (DEBUG) console.log("owner call approved, API: ", apiCall)
          //   try {
          //     const result = await this.ownerCalls[apiCall]!(request);
          //     if (DEBUG) console.log("owner call succeeded")
          //     if (DEBUG2) console.log("result of owner API call: ", result)
          //     return result
          //   } catch (error: any) {
          //     console.log("ERROR: owner call failed: ", error)
          //     return returnError(request, `API ERROR [${apiCall}]: ${error.message} \n ${error.stack}`, 500);
          //   }
          // } else {
          //   return returnError(request, ANONYMOUS_CANNOT_CONNECT_MSG, 401);
          // }
        } else if (this.visitorCalls[apiCall]) {
          // TODO: locked rooms should not be accessible until accepted
          // UPDATE: this is done at api call level, if locked and unless accepted, no api calls allowed
          // const data = jsonParseWrapper(msg.data.toString(), 'L733');
          // const _name: JsonWebKey = jsonParseWrapper(data.name, 'L578');
          // const isPreviousVisitor = sbCrypto.lookupKey(_name, this.visitors) >= 0;
          // const isAccepted = sbCrypto.lookupKey(_name, this.accepted_requests) >= 0;
          return await this.visitorCalls[apiCall]!(request, apiBody);
        } else if (this.adminCalls[apiCall]) {
          // these calls will self-authenticate
          return await this.adminCalls[apiCall]!(request, apiBody);
        } else {
          return returnError(request, "API endpoint not found: " + apiCall, 404)
        }
      } catch (error: any) {
        return returnError(request, `API ERROR [${apiCall}]: ${error.message} \n ${error.stack}`, 500);
      }
    });
  }

  // fetch most recent messages from local (worker) KV
  async #getRecentMessages(howMany: number, cursor = ''): Promise<Map<string, unknown>> {
    const listOptions: DurableObjectListOptions = { limit: howMany, prefix: this.channelId!, reverse: true };
    if (cursor !== '')
      // MTG: this is what we actually want
      listOptions.end = cursor; // not '.startAfter'

    // gets (lexicographically) latest 'howMany' keys
    const keys = Array.from((await this.storage.list(listOptions)).keys());
    // see this blog post for details on why we're setting allowConcurrency:
    // https://blog.cloudflare.com/durable-objects-easy-fast-correct-choose-three/
    const getOptions: DurableObjectGetOptions = { allowConcurrency: true };
    const messageList = await this.storage.get(keys, getOptions);
    
    // // we copy the Map we have in messageList, to a fresh map where each entry is converted with JSON.parse
    // // because the parallel get above returns un-parsed objects (obviously)
    // const messageMap = new Map<string, unknown>();
    // for (const [key, value] of messageList.entries())
    //   messageMap.set(key, JSON.parse(value as string));
    // // if (DEBUG) { console.log("getRecentMessages() messageList:"); console.log(messageMap) }
    // return messageMap;

    // update: we now return the raw messageList, and let the caller decide what to do with it
    return messageList
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
      blockedMessages: await this.#getRecentMessages(100), // we start "capturing"
      quit: false, // tracks cleanup, true means go away
      receivedUserInfo: false,
    };

    // track active connections
    this.sessions.push(session);

    webSocket.addEventListener("message", msg => {
      try {
        if (session.quit) {
          webSocket.close(1011, "WebSocket broken (got a quit).");
          return;
        }

        var message: ChannelMessage = {}
        if (typeof msg.data === "string") {
          message = jsonParseWrapper(msg.data.toString(), 'L594');
        } else if (msg.data instanceof ArrayBuffer) {
          message = extractPayload(msg.data).payload
        } else {
          throw new Error("Cannot parse contents type (not json nor arraybuffer)")
        }
        message = validate_ChannelMessage(message) // will throw if anything wrong

        if (DEBUG) {
          console.log("------------ getting websocket (event) msg from client ------------")
          if (DEBUG) {
            console.log(msg)
            console.log(message)
            console.log("-------------------------------------------------")
          }
        }

        if (message.ready) {
          // the client sends a "ready" message when it can start receiving
          if (DEBUG) console.log("got ready message from client")
          if (!session.blockedMessages) return;
          // Deliver all the messages we queued up since the user connected.
          // webSocket.send(JSON.stringify(session.blockedMessages))
          webSocket.send(assemblePayload(session.blockedMessages)!)
          session.blockedMessages.clear()
          return;
        }

        // Time stamps are monotonically increasing. We enforce that they must be different.
        // Stored as a string of [0-3] to facilitate prefix searches (within 4x time ranges).
        // We append "0000" for future needs, for example if we need above 1000 messages per second.
        // Can represent epoch timestamps for the next 400+ years.
        const tsNum = Math.max(Date.now(), this.lastTimestamp + 1);
        this.lastTimestamp = tsNum;
        this.storage.put('lastTimestamp', tsNum)
        const ts = tsNum.toString(4).padStart(22, "0") + "0000" // total length 26

        // TODO:all channel messages are verified before we forward

        // appending timestamp to channel id. this is global, unique message identifier
        // six '_' characters allow for owner to manage subchannels.
        const key = this.channelId + "______" + ts;

        // // TODO: last use of Dictioary :-)
        // const _x: Dictionary<string> = {}
        // _x[key] = msgData;
        // // We don't block on any of these: (ASYNC)
        // // Here is the main workhorse ... actually send the message to every listener
        // this.#broadcast(JSON.stringify(_x))

        this.#broadcast(assemblePayload(new Map([[key, msg.data]])))

        // and store it for posterity both local and global KV
        this.storage.put(key, msg.data);
        this.env.MESSAGES_NAMESPACE.put(key, msg.data);
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
      this.sessions = this.sessions.filter(member => member !== session);
    };
    webSocket.addEventListener("close", closeOrErrorHandler);
    webSocket.addEventListener("error", closeOrErrorHandler);

    // just a little ping that we're up and running
    webSocket.send(JSON.stringify({
      ready: true,
    }));
  }

  // broadcasts a message to all clients.
  async #broadcast(message: any) {
    if (DEBUG) console.log("broadcasting message: ", message)
    if (typeof message !== "string")
      message = JSON.stringify(message);

    // TODO: we don't send notifications for everything? for example locked-out messages?
    if (DEBUG2) console.log("calling sendWebNotifications()", message);
    await this.#sendWebNotifications(message);

    // Iterate over all the sessions sending them messages.
    this.sessions = this.sessions.filter(session => {
      if (session.name) {
        try {
          if (DEBUG) console.log("sending message to session: ", session.name)
          session.webSocket.send(message);
          return true;
        } catch (err) {
          if (DEBUG) console.log("ERROR: failed to send message to session: ", session.name)
          session.quit = true;
          return false; // delete session
        }
      } else {
        // This session hasn't sent the initial user info message yet, so we're not sending them
        // messages yet (no secret lurking!). Queue the message to be sent later.
        if (DEBUG) console.log("queueing message for session: ", message)
        session.blockedMessages.push(message);
        return true;
      }
    });
    // this.storage.put('ownerUnread', this.ownerUnread);
  }

  async #handleOldMessages(request: Request) {
    const { searchParams } = new URL(request.url);
    const currentMessagesLength = Number(searchParams.get('currentMessagesLength')) || 100;
    const cursor = searchParams.get('cursor') || '';
    const messageMap = await this.#getRecentMessages(currentMessagesLength, cursor);
    // let messageArray: { [key: string]: any } = {};
    // for (let [key, value] of messageMap)
    //   messageArray[key] = value;
    return returnResult(request, messageMap, 200);
  }

  async #getKey(type: string): Promise<string | null> {
    // if (this.personalRoom) {
    //   // keys managed by owner
    //   if (type === 'ledgerKey') return this.env.LEDGER_KEY;
    //   return await this.storage.get(type) || null;
    // }

    // // otherwise it's keys managed by SSO / server
    // if (type === 'ownerKey') {
    //   const _keys_id = (await this.env.KEYS_NAMESPACE.list({ prefix: this.channelId + '_ownerKey' })).keys.map(key => key.name);
    //   if (_keys_id.length == 0) return null;
    //   const keys = _keys_id.map(async key => await this.env.KEYS_NAMESPACE.get(key));
    //   const keyResult = await keys[keys.length - 1];
    //   _sb_assert(keyResult, "ERROR: no owner key found (fatal)");
    //   return keyResult!;
    // } else if (type === 'ledgerKey') {
    //   return await this.env.KEYS_NAMESPACE.get(type);
    // }

    switch (type) {
      case 'ledgerKey':
        return this.env.LEDGER_KEY
      default:
        return await this.storage.get(type) || null;
    }

    // return await this.env.KEYS_NAMESPACE.get(this.channelId + '_' + type);
  }

  // // deprecated (and this was flawed), see notes in jslib
  // async #postPubKey(request: Request) {
  //   return returnError(request, "postPubKey is deprecated", 400)
  // }

  async #handleChannelCapacityChange(request: Request) {
    const { searchParams } = new URL(request.url);
    const newLimit = searchParams.get('capacity');
    this.channelCapacity = Number(newLimit) || this.channelCapacity;
    this.storage.put('room_capacity', this.channelCapacity)
    return returnResultJson(request, { capacity: newLimit }, 200);
  }

  // async #getChannelCapacity(request: Request) {
  //   return returnResultJson(request, { capacity: this.channelCapacity }, 200);
  // }

  // async #getOwnerUnreadMessages(request: Request) {
  //   return returnResultJson(request, { unreadMessages: this.ownerUnread }, 200);
  // }

  // async #getPubKeys(request: Request) {
  //   return returnResult(request, JSON.stringify({ keys: this.visitors }), 200);
  // }

  async #acceptVisitor(request: Request, apiBody: ChannelApiBody) {
    _sb_assert(apiBody.apiPayload, "acceptVisitor(): need to provide userId")
    const data = extractPayload(apiBody.apiPayload!).payload
    if (data && data.userId && typeof data.userId === 'string') {
      if (!this.accepted.has(data.userId)) {
        if (this.accepted.size >= this.channelCapacity)
          return returnError(request, `This would exceed current channel capacity (${this.channelCapacity}); update that first`, 400)
        // add to our accepted list
        this.accepted.add(data.userId)
        // write it back to storage
        await this.storage.put('accepted', assemblePayload(this.accepted))
      }
      return returnResultJson(request, { success: true }, 200);
    } else {
      return returnError(request, "acceptVisitor(): could not parse the provided userId", 400)
    }

    // const data = await request.json();
    // // const acceptPubKey: JsonWebKey = jsonParseWrapper((data as any).pubKey, 'L783');
    // const userId: SBUserId = (data as any).userId
    // // const ind = this.join_requests.indexOf((data as any).pubKey as string);
    // // const ind = sbCrypto.lookupKey(acceptPubKey, this.join_requests);
    // const ind = this.joinRequests.indexOf(userId)
    // if (ind >= 0) {
    //   this.acceptedRequests = [...this.acceptedRequests, ...this.joinRequests.splice(ind, 1)];
    //   if (DEBUG) console.log("Encrypted locked keys:", this.encryptedLockedKeys)
    //   this.encryptedLockedKeys.set((data as any).pubKey as string, (data as any).encryptedLockedKey);
    //   this.storage.put('accepted_requests', JSON.stringify(this.acceptedRequests));
    //   this.storage.put('encryptedLockedKeys', this.encryptedLockedKeys);
    //   this.storage.put('join_requests', JSON.stringify(this.joinRequests))
    //   return returnResultJson(request, { success: true}, 200);
    // } else {
    //   return returnError(request, "Could not accept visitor (visitor not found)", 400)
    // }
  }

  async #lockChannel(request: Request, _apiBody: ChannelApiBody) {
    // ToDo: shut down any open websocket sessions
    this.locked = true;

    // for (let i = 0; i < this.visitors.length; i++) {
    //   const thisVisitor = this.visitors[i];
    //   if (thisVisitor) {
    //     if (this.acceptedRequests.indexOf(thisVisitor) < 0 && this.joinRequests.indexOf(thisVisitor) < 0)
    //       this.joinRequests.push(thisVisitor);
    //   }
    // }
    // this.storage.put('join_requests', JSON.stringify(this.joinRequests));

    this.storage.put('locked', this.locked)

    return returnResultJson(request, { success: true }, 200);
  }

  // async #getJoinRequests(request: Request) {
  //   return returnResult(request, JSON.stringify({ join_requests: this.joinRequests }), 200);
  // }

  async #isChannelLocked(request: Request) {
    return returnResultJson(request, { locked: this.locked }, 200);
  }

  // async #setMOTD(request: Request) {
  //   const data = await request.json();
  //   this.motd = (data as any).motd;
  //   this.storage.put('motd', this.motd);
  //   return returnResult(request, JSON.stringify({ motd: this.motd }), 200);
  // }

  // serializeMap = (map: Map<string, string>): string => JSON.stringify(Array.from(map.entries()))
  // deserializeMap = (jsonStr: string): Map<string, string> => new Map(JSON.parse(jsonStr));

  // // TODO: we do not allow owner key rotations at the moment, but we need to add
  // // regular key rotation(s), so keeping this as template code
  // async #ownerKeyRotation(request: Request, _apiBody: ChannelApiBody) {
  //   return returnError(request, "Owner key is deprecated", 405)
    // if (ALLOW_OWNER_KEY_ROTATION) {
    //   let _tries = 3;
    //   let _timeout = 10000;
    //   let _success = await this.#checkRotation(1);
    //   if (!_success) {
    //     while (_tries > 0) {
    //       _tries -= 1;
    //       _success = await this.#checkRotation(_timeout)
    //       if (_success) {
    //         break;
    //       }
    //       _timeout *= 2;
    //     }
    //     if (!_success) {
    //       return returnResult(request, JSON.stringify({ success: false }), 200);
    //     }
    //   }
    //   // const _updatedKey = await this.getKey('ownerKey');
    //   const _updatedKey = this.room_owner; // ToDo: user new ownerId format
    //   // ehm .. why was this removed?
    //   this.#broadcast(JSON.stringify({ control: true, ownerKeyChanged: true, ownerKey: _updatedKey }));
    //   this.room_owner = _updatedKey;

    //   // Now pushing all accepted requests back to join requests
    //   this.join_requests = [...this.join_requests, ...this.accepted_requests];
    //   this.accepted_requests = [];
    //   // this.lockedKeys = [];
    //   this.encryptedLockedKeys = new Map();
    //   this.storage.put('join_requests', JSON.stringify(this.join_requests))
    //   // this.storage.put('lockedKeys', JSON.stringify(this.lockedKeys))
    //   this.storage.put('encryptedLockedKeys', this.serializeMap(this.encryptedLockedKeys))
    //   this.storage.put('accepted_requests', JSON.stringify(this.accepted_requests));
    //   return returnResult(request, JSON.stringify({ success: true }), 200);
    // } else {
    //   return returnError(request, "Owner key rotation not allowed", 405);
    // }
  //}

  // // clumsy event handling to track change; TODO cleanup
  // async #checkRotation(_timeout: number): Promise<boolean> {
  //   await new Promise((resolve) => setTimeout(
  //     () => {
  //       resolve(true);
  //     }, _timeout));
  //   return (true)
  // }

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
    // if (this.locked) {
    //   // ToDo: need test cases
    //   const data = jsonParseWrapper(new TextDecoder().decode(await request.arrayBuffer()), 'L976') || {}
    //   // const isAccepted = sbCrypto.lookupKey(data.userId, this.accepted_requests) >= 0;
    //   const isAccepted = this.accepted_requests.includes(data.userId)
    //   if (!isAccepted)
    //     return returnError(request, '[/storageRequest]: Either no such channel or you are not authorized', 401);
    // }
    const { searchParams } = new URL(request.url);
    const size = this.#roundSize(Number(searchParams.get('size')));
    const storageLimit = this.storageLimit;
    if (size > storageLimit) return returnError(request, 'Not sufficient storage budget left in channel', 507);
    if (size > serverConstants.STORAGE_SIZE_MAX) return returnResult(request, `Storage size too large (max ${serverConstants.STORAGE_SIZE_MAX} bytes)`, 413);

    this.storageLimit = storageLimit - size;
    this.storage.put('storageLimit', this.storageLimit); // here we've consumed it
    const token = arrayBufferToBase64(crypto.getRandomValues(new Uint8Array(48)).buffer);
    await this.env.LEDGER_NAMESPACE.put(token,
      JSON.stringify(
        {
          used: false,
          size: size,
          motherChannel: this.channelId,
          created: Date.now()
        }
      ));

    // const token_buffer = crypto.getRandomValues(new Uint8Array(48)).buffer;
    // const token_hash_buffer = await crypto.subtle.digest('SHA-256', token_buffer)
    // const token_hash = arrayBufferToBase64(token_hash_buffer);
    // const kv_data = { used: false, size: size, motherChannel: this.channelId };
    // await this.env.LEDGER_NAMESPACE.put(token_hash, JSON.stringify(kv_data));
    // const encrypted_token_id = arrayBufferToBase64(await crypto.subtle.encrypt({ name: "RSA-OAEP" }, this.ledgerKey!, token_buffer));
    // const hashed_room_id = arrayBufferToBase64(await crypto.subtle.digest('SHA-256', (new TextEncoder).encode(this.channelId)));
    // const token = {
    //   token_hash: token_hash,
    //   hashed_room_id: hashed_room_id,
    //   encrypted_token_id: encrypted_token_id
    // };

    if (DEBUG) console.log(`[newStorage()]: Created new storage token (${token.slice(0, 12)}...) for ${size} bytes`);
    return returnResult(request, JSON.stringify(token), 200);
  }

  async #getStorageLimit(request: Request) {
    return returnResult(request, JSON.stringify({ storageLimit: this.storageLimit }), 200);
  }

  async #getMother(request: Request) {
    return returnResult(request, JSON.stringify({ motherChannel: this.motherChannel }), 200);
  }

  /*
     Transfer storage budget from one channel to another. Use the target
     channel's budget, and just get the channel ID from the request
     and look up and increment it's budget.
  */
  async #handleBuddRequest(request: Request): Promise<Response> {
    if (DEBUG2) console.log(request)
    const { searchParams } = new URL(request.url);
    const targetChannel = searchParams.get('targetChannel');

    if (!targetChannel)
      return returnError(request, '[budd()]: No target channel specified', 400);
    if (this.channelId === targetChannel)
      return returnResult(request, JSON.stringify({ success: true }), 200); // no-op
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

    jsonData["SERVER_SECRET"] = this.env.SERVER_SECRET; // authorizing this creation/transfer; ToDo: should not propagate auth in this way
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
      return returnResult(request, adminData, 200);
    } catch (err) {
      if (DEBUG) console.log("[#handleAdminDataRequest] Error:", err)
      throw err
    }
  }

  #registerDevice(request: Request) {
    return returnError(request, "registerDevice is disabled, use web notifications", 400)
  }

  // TODO: review this, it sends a return value that is not used
  async #sendWebNotifications(message: string) {
    const envNotifications = this.env.notifications
    if (!envNotifications) {
      if (DEBUG) console.log("Cannot send web notifications (expected behavior if you're running locally")
      return;
    }
    if (DEBUG) console.log("Sending web notification", message)
    message = jsonParseWrapper(message, 'L999')
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
          "Content-Type": "application/json"
        }
      }
      // console.log("Sending web notification", options)
      // ToDo: needs to be environment setting not static
      // return await envNotifications.fetch("https://notifications.384.dev/notify", options)
      // MTG: we are reaching to a heroku endpoint here not a cloudflare one, CF doesnt support http2 fetch for notification servers
      return await fetch("https://notify.384.dev/notify", options)
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

  // async #createNewChannel(request: Request): Promise<Response> {
  //   if (DEBUG) console.log("==== createNewChannel() ====")
  //   const resp = await this.#createChannel(request);
  //   if (!resp) return returnError(request, "Failed to create channel", 500);
  //   return resp;
  // }


  async #createChannel(request: Request, apiBody: ChannelApiBody): Promise<Response | null> {
    // request cloning is done by callee

    // const url = new URL(request.url);
    // const path = url.pathname.slice(1).split('/');
    // if (!path || path.length === 0) return returnError(request, "Invalid path, missing new channel ID", 400);
    // const newChannelId = path[0]; _sb_assert(newChannelId, "ERROR: no new channel ID");
    // const jsonString = new TextDecoder().decode(await request.arrayBuffer());
    // if (DEBUG) {
    //   console.log(`==== createChannel(${newChannelId}) ====`)
    //   console.log(jsonString)
    //   console.log("========================================")
    // }
    // const _cd: SBChannelData = jsonParseWrapper(jsonString, 'L1303')
    // _sb_assert(_cd.channelId && _cd.ownerPublicKey && _cd.storageToken, "ERROR: invalid channel data")

    var _cd: SBChannelData = extractPayload(apiBody.apiPayload!).payload
    _cd = validate_SBChannelData(_cd) // will throw if anything wrong
    _sb_assert(_cd.storageToken, "[createChannel()] storageToken missing")
    const newChannelId = _cd.channelId

    const _storage_token_hash = await this.env.LEDGER_NAMESPACE.get(_cd.storageToken!);
    const _ledger_resp = _storage_token_hash ? jsonParseWrapper(_storage_token_hash, 'L1307') : null;
    if (!_ledger_resp) return returnError(request, `Having issues processing storage token ${_cd.storageToken}, got '${_storage_token_hash}'`, 507);
    if (_ledger_resp.used) return returnError(request, "Storage token already used", 507);
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
    await this.storage.put('storageLimit', storageTokenSize); // and now new channel can spend it
    if (DEBUG) console.log("++++ createChannel() from token: starting size: ", storageTokenSize)

    // // let newOwnerKeyJson: JsonWebKey;
    // if () {
    //   const _jwk = sbCrypto.StringToJWK(jsonData["userKeyString"]);
    //   if (!_jwk) return returnError(request, "Invalid user (owner) key (could not import 'userKeyString')", 400);
    //   else newOwnerKeyJson = _jwk;
    // } else if (jsonData["ownerKey"]) {

    // } else {
    //   return returnError(request, "No owner key provided (need either 'userKeyString' or older interface 'owneKey')", 400);
    // }

    // // new interface converts userKeyString first to JWK and then JSON string ... below we work from JSON string
    // const newOwnerKey = jsonData["ownerKey"] ?? JSON.stringify();
    // if (!newOwnerKey)
    //   return returnError(request, "No owner key provided", 400);

    // const newOwnerKeyJson = jsonParseWrapper(newOwnerKey, 'L1218');

    // if (!(await sbCrypto.verifyChannelId(newOwnerKeyJson, newChannelId))) {
    //   if (DEBUG) {
    //     console.log("createChannel(): newOwnerKey: ", newOwnerKey)
    //     console.log("createChannel(): generated ID: ")
    //     console.log(await sbCrypto.sb384Hash(newOwnerKeyJson))
    //     console.log("createChannel(): newChannelId: ")
    //     console.log(path[0])
    //   }
    //   return returnError(request, "Owner key does not match channel id (validation of channel viz keys failed)", 400);
    // }


    // for (const key of ["ownerKey", "encryptionKey", "signKey", "motherChannel", "visitors"]) {
    //   const newData = jsonData[key];
    //   if (newData) {
    //     if (DEBUG2) console.log("++ createChannel(): putting key, value: ", key, newData)
    //     await this.storage.put(key, newData);
    //   }
    // }
    // await this.storage.put("personalRoom", 'true');

    // // signal a new room - this will be picked up by "#initialize"
    // await this.storage.put("storageLimit", Infinity);

    // note that for a new room, "initialize" will fetch data from "this.storage" into object
    await this
      .#initialize(_cd)
      .catch(err => { return returnError(request, `Error initializing room [L1385]: ${err}`, 500) });
    if (DEBUG) console.log("++++ CREATED channel:", this.#describe());
    return null; // null means no errors
  }

  async #getChannelKeys(request: Request) {
    if (!this.channelData)
      return returnError(request, "Channel keys ('ChannelData') not initialized", 500);
    else
      // return returnResult(request, JSON.stringify(this.#channelKeys!.channelData), 200);
      return returnResult(request, JSON.stringify(this.channelData), 200);

    // const data: any = {};
    // data.ownerKey = this.#channelKeyStrings.ownerKey;
    // if (this.#channelKeyStrings.guestKey)
    //   data.guestKey = this.#channelKeyStrings.guestKey;
    // data.encryptionKey = this.#channelKeyStrings!.encryptionKey;
    // data.signKey = this.#channelKeyStrings.signKey;
    // if (this.locked) {
    //   if (DEBUG) {
    //     console.log("getChannelKeys(): locked... body:")
    //     console.log(request.body)
    //   }
    //   // encryptedLockedKeys .. TODO .. working on this
    //   // const encryptedLockedKey = this.encryptedLockedKeys.get(this.) || null;
    //   // if (encryptedLockedKey) {
    //   //   data.encryptedLockedKey = encryptedLockedKey;
    //   // }
    // }
    // // if (this.#channelKeyStrings.encryptedLockKey) {
    // //   data.lockedKey = this.#channelKeyStrings.encryptedLockKey;
    // // }
    // return returnResult(request, JSON.stringify(data), 200);

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
        // we only allow imports here of keys that correspond to messages.
        // in the json they'll look something like:
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
      return returnResult(request, JSON.stringify({ success: true }), 200);
    } else {
      if (DEBUG) console.log("uploadData() not allowed (room might be partially created)")
      return returnError(request, ANONYMOUS_CANNOT_CONNECT_MSG, 401);
    }
  }

  // async #authorizeChannel(request: Request) {
  //   const _secret = this.env.SERVER_SECRET;
  //   const jsonData: any = await request.json();
  //   const requestAuthorized = jsonData.hasOwnProperty("SERVER_SECRET") && jsonData["SERVER_SECRET"] === _secret;
  //   if (requestAuthorized) {
  //     // for (const key in jsonData) { } // TODO: any other keys to check?
  //     // this.personalRoom = true;
  //     // await this.storage.put("personalRoom", 'true');
  //     this.room_owner = jsonData["room_owner"];
  //     await this.storage.put("room_owner", jsonData["ownerKey"]);
  //     return returnResult(request, JSON.stringify({ success: true }), 200);
  //   } else {
  //     return returnError(request, "Cannot authorize room: server secret did not match", 401);
  //   }
  // }
}
