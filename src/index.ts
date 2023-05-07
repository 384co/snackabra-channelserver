/*
   Copyright (C) 2019-2021 Magnusson Institute, All Rights Reserved

   "Snackabra" is a registered trademark

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

/* 384co internal note: scratch code in e062 */

const DEBUG = true;
const DEBUG2 = false;

if (DEBUG) console.log("++++ channel.mjs loaded ++++ DEBUG is enabled ++++")
if (DEBUG2) console.log("++++ DEBUG2 (verbose) enabled ++++")

// TODO: future refactor will be calculating internally in units
//       of 4KB bytes. This allows for (2^64) bytes storage per channel.
//       Also, future change will allocate any object budgeted by a 
//       channel an "address", eg so that everything, ever allocated
//       by one channel could be conceived of as a single heap, 
//       within which the start of any shard can be addressed by a 
//       52-bit "pointer" to a 4KB boundary (and thus can be kept
//       safely in a Javascript Number). 

const STORAGE_SIZE_UNIT = 4096;

// Currently minimum (raw) storage is set to 32KB. This will not
// be LOWERED, but future design changes may RAISE that. 
const STORAGE_SIZE_MIN = 8 * STORAGE_SIZE_UNIT;

// Current maximum (raw) storage is set to 32MB. This may change.
const STORAGE_SIZE_MAX = 8192 * STORAGE_SIZE_UNIT;

// minimum when creating (budding) a new channel
const NEW_CHANNEL_MINIMUM_BUDGET = 32 * 1024 * 1024; // 8 MB

// new channel budget (bootstrap) is 3 GB (about $1)
const NEW_CHANNEL_BUDGET = 3 * 1024 * 1024 * 1024; // 3 GB

// sanity check - set a max at one petabyte (2^50)
const MAX_BUDGET_TRANSFER = 1024 * 1024 * 1024 * 1024 * 1024; // 1 PB

// see notes in jslib on owner key rotation
const ALLOW_OWNER_KEY_ROTATION = false;

import {
  arrayBufferToBase64, base64ToArrayBuffer,
  jsonParseWrapper,
  ChannelKeys, SBChannelId,
  SBCrypto,
  ChannelAdminData
} from './snackabra.js';

const sbCrypto = new SBCrypto()

// this section has some type definitions that helps us with CF types
type EnvType = {
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
}

// Reminder of response codes we use:
//
// 101: Switching Protocols (downgrade error)
// 200: OK
// 400: Bad Request
// 401: Unauthorized
// 403: Forbidden
// 404: Not Found
// 405: Method Not Allowed
// 418: I'm a teapot
// 429: Too Many Requests
// 500: Internal Server Error
// 501: Not Implemented
// 507: Insufficient Storage (WebDAV/RFC4918)
//
type ResponseCode = 101 | 200 | 400 | 401 | 403 | 404 | 405 | 418 | 429 | 500 | 501 | 507;

// this handles UNEXPECTED errors
async function handleErrors(request: Request, func: () => Promise<Response>) {
  try {
    return await func();
  } catch (err: any) {
    if (request.headers.get("Upgrade") == "websocket") {
      let pair = new WebSocketPair();
      pair[1].accept();
      pair[1].send(JSON.stringify({ error: '[handleErrors()] ' + err.message + '\n' + err.stack }));
      pair[1].close(1011, "Uncaught exception during session setup");
      console.log("webSocket close (error)")
      return returnResult(request, null, 101, 0);
    } else {
      return returnResult(request, err.stack, 500)
    }
  }
}

function returnResult(_request: Request, contents: any, s: ResponseCode, delay = 0) {
  const corsHeaders = {
    "Access-Control-Allow-Methods": "POST, OPTIONS, GET",
    "Access-Control-Allow-Headers": "Content-Type, authorization",
    "Access-Control-Allow-Credentials": "true",
    "Access-Control-Allow-Origin": "*" /* request.headers.get("Origin") */,
    "Content-Type": "application/json;",
  }
  return new Promise<Response>((resolve) => {
    setTimeout(() => {
      if (DEBUG) console.log("returnResult() contents:", contents, "status:", s)
      resolve(new Response(contents, { status: s, headers: corsHeaders }));
    }, delay);
  });
}

function returnError(_request: Request, errorString: string, status: ResponseCode) {
  console.log("ERROR: (status: " + status + ")\n" + errorString);
  return returnResult(_request, `{ "error": "${errorString}" }`, status);
}

/**
 * API calls are in one of two forms:
 * 
 * ::
 * 
 *     /api/<api_call>/
 *     /api/room/<id>/<api_call>/
 * 
 * The first form is asynchronous, the latter is synchronous.
 * A 'sync' call means that there's only a single server endpoint
 * that is handling calls. The channel id thus constitutes
 * the point of synchronization.
 * 
 * Currently, api calls are strictly one or the other. That will
 * likely change.
 * 
 * Finally, one api endpoint is special:
 * 
 * ::
 * 
 *     /api/room/<id>/websocket
 * 
 * Which will upgrade protocol to a websocket connection.
 * 
 * Previous design was divided into separate shard and channel
 * servers, but this version is merged. For historical continuity,
 * below we divide them into shard and channel calls.
 * 
 * ::
 * 
 *     Shard API:
 *     /api/storeRequest/
 *     /api/storeData/
 *     /api/fetchData/
 *     /api/migrateStorage/
 *     /api/fetchDataMigration/
 *
 *     Channel API (async):
 *     /api/notifications/       : sign up for notifications (disabled)
 *     /api/getLastMessageTimes/ : queries multiple channels for last message timestamp
 *
 *     Channel API (synchronous)          : [O] means [Owner] only
 *     /api/room/<ID>/websocket           : connect to channel socket (wss protocol)
 *     /api/room/<ID>/oldMessages
 *     /api/room/<ID>/updateRoomCapacity  : [O]
 *     /api/room/<ID>/budd                : [O]
 *     /api/room/<ID>/getStorageLimit     : [O]
 *     /api/room/<ID>/getMother           : [O]
 *     /api/room/<ID>/getRoomCapacity     : [O]
 *     /api/room/<ID>/getPubKeys          : [O]
 *     /api/room/<ID>/getJoinRequests     : [O]
 *     /api/room/<ID>/lockRoom            : [O]
 *     /api/room/<ID>/acceptVisitor       : [O]
 *     /api/room/<ID>/roomLocked
 *     /api/room/<ID>/ownerUnread         : [O]
 *     /api/room/<ID>/motd                : [O]
 *     /api/room/<ID>/ownerKeyRotation    : [O] (deprecated)
 *     /api/room/<ID>/storageRequest
 *     /api/room/<ID>/getAdminData        : [O]
 *     /api/room/<ID>/registerDevice      : (disabled)
 *     /api/room/<ID>/downloadData
 *     /api/room/<ID>/uploadRoom          : (admin only)
 *     /api/room/<ID>/authorizeRoom       : (admin only)
 *     /api/room/<ID>/postPubKey
 * 
 */
export default {
  async fetch(request: Request, env: EnvType) {
    if (DEBUG) console.log(`fetch called: ${request.url}`)
    return await handleErrors(request, async () => {

      let url = new URL(request.url);
      let path = url.pathname.slice(1).split('/');

      switch (path[0]) {
        case "api":
          // This is a request for `/api/...`, call the API handler.
          return handleApiRequest(path.slice(1), request, env);

        default:
          return returnResult(request, JSON.stringify({ error: "Not found (must give API endpoint)" }), 404)
        //return new Response("Not found", { status: 404 });
      }
    });
  }
}

// 'name' is room/channel name, 'path' is the rest of the path
async function callDurableObject(name: SBChannelId, path: Array<string>, request: Request, env: EnvType) {
  if (DEBUG) {
    console.log("callDurableObject() name:", name, "path:", path)
    if (DEBUG2) { console.log(request); console.log(env) }
  }
  // const pubKey = await fetch("https://m063.dpn.workers.dev/api/v1/pubKeys?roomId=" + name)
  // if (pubKey?.error) return returnResult(request, JSON.stringify({ error: "Not found" }), 404);
  let roomId = env.channels.idFromName(name);
  let roomObject = env.channels.get(roomId);
  let newUrl = new URL(request.url);
  newUrl.pathname = "/" + name + "/" + path.join("/");
  if (DEBUG2) { console.log("callDurableObject() newUrl:"); console.log(newUrl); }
  return roomObject.fetch(newUrl, request);
}

// 'path' is the request path, starting AFTER '/api'
async function handleApiRequest(path: Array<string>, request: Request, env: EnvType) {
  if (DEBUG) { console.log(`handleApiRequest() path:`); console.log(path); }
  try {
    switch (path[0]) {
      case "room":
      case "channel":
        {
          return callDurableObject(path[1], path.slice(2), request, env);
        }
      case "notifications":
        {
          return returnError(request, "Device (Apple) notifications are disabled (use web notifications)", 400);
        }
      case "getLastMessageTimes":
        {
          try {
            const _rooms: any = await request.json();
            let lastMessageTimes: Array<any> = [];
            for (let i = 0; i < _rooms.length; i++) {
              lastMessageTimes[_rooms[i]] = await lastTimeStamp(_rooms[i], env);
            }
            return returnResult(request, JSON.stringify(lastMessageTimes), 200);
          } catch (error: any) {
            console.log(error);
            return returnResult(request, JSON.stringify({ error: '[getLastMessageTimes] ' + error.message + '\n' + error.stack }), 500);
          }
        }
      default:
        return returnResult(request, JSON.stringify({ error: "Not found (this is an API endpoint, the URI was malformed)" }), 404)
    }
  } catch (error: any) {
    console.log(error);
    return returnResult(request, JSON.stringify({ error: `[API Call error] [${request.url}]: \n` + error.message + '\n' + error.stack }), 500);
  }
}

async function lastTimeStamp(room_id: SBChannelId, env: EnvType) {
  try {
    let list_response = await env.MESSAGES_NAMESPACE.list({ "prefix": room_id });
    let message_keys: any = list_response.keys.map((res) => {
      return res.name
    });
    if (message_keys.length === 0) {
      return '0'
    }
    while (!list_response.list_complete) {
      list_response = await env.MESSAGES_NAMESPACE.list({ "cursor": list_response.cursor })
      message_keys = [...message_keys, list_response.keys];
    }
    return message_keys[message_keys.length - 1].slice(room_id.length);
  } catch (error) {
    console.log(error);
    return '0';
  }
}

function _assert(val: unknown, msg: string) {
  if (!!val) throw new Error('assertion failed: ' + msg)
}

interface Dictionary<T> {
  [index: string]: T;
}

// this helps force an error object to be an error object
function WrapError(e: unknown) {
  if (e instanceof Error) {
    return e;
  } else {
    return new Error(String(e));
  }
}

// Decorator: ready pattern
function Ready(target: any, propertyKey: string, descriptor: PropertyDescriptor) {
  if (descriptor.get) {
    // console.log("Checking ready for:")
    // console.log(target.constructor.name)
    let get = descriptor.get
    descriptor.get = function () {
      // console.log("============= Ready checking this:")
      // console.log(this)
      const obj = target.constructor.name
      // TODO: the Ready template can probably be simplified
      const prop = `${obj}ReadyFlag`
      if (prop in this) {
        // console.log(`============= Ready() - checking readyFlag for ${propertyKey}`)
        const rf = "readyFlag" as keyof PropertyDescriptor
        // console.log(this[rf])
        _assert(this[rf], `${propertyKey} getter accessed but object ${obj} not ready (fatal)`)
      }
      const retValue = get.call(this)
      _assert(retValue != null, `${propertyKey} getter accessed in object type ${obj} but returns NULL (fatal)`)
      return retValue
    }
  }
}

type ApiCallMap = {
  [key: string]: ((arg0: Request) => Promise<Response>) | undefined;
};

type SessionType = {
  name: string,
  room_id: SBChannelId,
  webSocket: WebSocket,
  blockedMessages: Map<string, unknown>,
  quit: boolean,
  receivedUserInfo: boolean
}

/**
 *
 * ChannelServer Durable Object Class
 * 
 * One instance per channel/room.
 *     
 * Note: historically channels were referred to as 'rooms'.
 */
export class ChannelServer implements DurableObject {
  storage: DurableObjectStorage;
  env: EnvType;

  initializePromise: Promise<boolean> | null = null;

  sessions: Array<any> = [];
  #channelKeys?: ChannelKeys

  room_id: SBChannelId = '';
  room_owner: string | null = null; // duplicate, placeholder / TODO cleanup

  // config?: ChannelConfig;

  ownerCalls: ApiCallMap;
  visitorCalls: ApiCallMap;
  adminCalls: ApiCallMap;

  // We keep track of the last-seen message's timestamp just so that we can assign monotonically
  // increasing timestamps even if multiple messages arrive simultaneously (see below).
  lastTimestamp: number = 0;

  storageLimit: number = 0;
  verified_guest: string = '';

  visitors: Array<JsonWebKey> = [];
  join_requests: Array<JsonWebKey> = [];
  accepted_requests: Array<JsonWebKey> = [];
  // tracks history of lock keys
  lockedKeys: Array<JsonWebKey> = [];

  room_capacity: number = 20;
  ownerUnread: number = 0;
  locked: boolean = false;
  motd: string = '';
  ledgerKey: CryptoKey | null = null;
  personalRoom: boolean = false;

  // these are in #channelKeys (TODO cleanup)
  // room_owner: JsonWebKey | null = null;
  // verified_guest: string | null = null;
  // encryptionKey: string | null = null;
  // signKey: string | null = null;

  // 2023.05.xx: new fields
  motherChannel: string = '';
  messagesCursor: string | null = null; // used for 'startAfter' option

  constructor(state: DurableObjectState, env: EnvType) {
    // NOTE:
    // durable object storage has a different API than global KV, see:
    // https://developers.cloudflare.com/workers/runtime-apis/durable-objects/#transactional-storage-api
    this.storage = state.storage;
    this.env = env;

    this.ownerCalls = {
      "/acceptVisitor": this.acceptVisitor.bind(this),
      "/budd": this.handleBuddRequest.bind(this),
      "/getAdminData": this.handleAdminDataRequest.bind(this),
      "/getJoinRequests": this.getJoinRequests.bind(this),
      "/getMother": this.getMother.bind(this),
      "/getPubKeys": this.getPubKeys.bind(this),
      "/getRoomCapacity": this.getRoomCapacity.bind(this),
      "/getStorageLimit": this.getStorageLimit.bind(this),
      "/lockRoom": this.lockRoom.bind(this),
      "/motd": this.setMOTD.bind(this),
      "/ownerKeyRotation": this.ownerKeyRotation.bind(this), // deprecated
      "/ownerUnread": this.getOwnerUnreadMessages.bind(this),
      "/updateRoomCapacity": this.handleRoomCapacityChange.bind(this),
    }
    this.visitorCalls = {
      "/downloadData": this.downloadAllData.bind(this),
      "/oldMessages": this.handleOldMessages.bind(this),
      "/postPubKey": this.postPubKey.bind(this), // deprecated
      "/registerDevice": this.registerDevice.bind(this), // deprecated
      "/roomlocked": this.isRoomLocked.bind(this),
      "/storageRequest": this.handleNewStorage.bind(this),
    }
    this.adminCalls = {
      "/authorizeRoom": this.authorizeRoom.bind(this),
      "/uploadRoom": this.uploadData.bind(this)
    }
  }

  // need the initialize method to restore state of room when the worker is updated
  initialize(room_id: SBChannelId): Promise<boolean> {
    // TODO: this cannot handle exceptions ... 
    return new Promise((resolve, reject) => {
      // let ledgerKeyString = await this.getKey('ledgerKey');
      let ledgerKeyString = this.env.LEDGER_KEY;
      if (ledgerKeyString != null && ledgerKeyString !== "") {
        // ledger is RSA-OAEP so we do not use sbCrypto
        crypto.subtle.importKey("jwk",
          jsonParseWrapper(ledgerKeyString, 'L217'),
          { name: "RSA-OAEP", hash: 'SHA-256' },
          true, ["encrypt"]).then((ledgerKey) => {
            const storage = this.storage;
            // this.config = new ChannelConfig(room_id, ledgerKey, storage);
            if (DEBUG) {
              console.log("Done creating room:")
              console.log("room_id: ", this.room_id)
              if (DEBUG2) console.log(this)
            }
            resolve(true);
          });
      } else {
        reject("Failed to initialize ChannelServer (no ledgerKey?)")
      }
    });
  }

  async fetch(request: Request) {
    if (!this.initializePromise) {
      if (DEBUG) {
        console.log("forcing an initialization of new room:\n");
        console.log(request.url)
      }
      this.initializePromise = this.initialize((new URL(request.url)).pathname.split('/')[1]);
    }
    // make sure "we" are ready to go
    await this.initializePromise;
    if (this.verified_guest === '') {
      this.verified_guest = await this.getKey('guestKey') || '';
    }
    return await handleErrors(request, async () => {
      let url = new URL(request.url);

      // // SSO code - this would optionally verify public key from a public record room->key
      // if (this.room_owner === '') {
      //   this.room_id = url.pathname.split('/')[1];
      //   const keyFetch_json = await fetch("https://m063.dpn.workers.dev/api/v1/pubKeys?roomId=" + this.room_id)
      //   this.room_owner = JSON.stringify(keyFetch_json.ownerKey);
      //   this.encryptionKey = JSON.stringify(keyFetch_json.encryptionKey);
      // }

      this.room_id = url.pathname.split('/')[1];
      // url.pathname = "/" + url.pathname.split('/').slice(2).join("/");
      let url_pathname = "/" + url.pathname.split('/').slice(2).join("/")
      let new_url = new URL(url.origin + url_pathname)
      if (DEBUG2) {
        console.log("fetch() top new_url: ", new_url)
        console.log("fetch() top new_url.pathname: ", new_url.pathname)
      }

      // API section, all *synchronous* API calls are routed trough here
      const apiCall = new_url.pathname
      try {
        if (apiCall === "/websocket") {
          if (request.headers.get("Upgrade") != "websocket")
            return returnError(request, "Expected websocket", 400);
          let ip = request.headers.get("CF-Connecting-IP");
          let pair = new WebSocketPair();
          await this.handleSession(pair[1], ip);
          return new Response(null, { status: 101, webSocket: pair[0] });
        } else if (this.ownerCalls[apiCall]) {
          if (await this.verifyAuth(request)) {
            return await this.ownerCalls[apiCall]!(request);
          } else {
            return returnResult(request, JSON.stringify({ error: "Owner verification failed (restricted API call)" }), 401);
          }
        } else if (this.visitorCalls[apiCall]) {
          return await this.visitorCalls[apiCall]!(request);
        } else if (this.adminCalls[apiCall]) {
          // these calls will self-authenticate
          return await this.adminCalls[apiCall]!(request);
        } else {
          return returnError(request, "API endpoint not found: " + apiCall, 404)
        }
      } catch (error: any) {
        return returnError(request, `API ERROR [${apiCall}]: ${error.message} \n ${error.stack}`, 500);
      }
    });
  }

  // fetch most recent messages from local (worker) KV
  async getRecentMessages(howMany: number, cursor = ''): Promise<Map<string, unknown>> {
    let listOptions: DurableObjectListOptions = { limit: howMany, prefix: this.room_id, reverse: true };
    if (cursor !== '')
      listOptions.startAfter = cursor;
    let keys = Array.from((await this.storage.list(listOptions)).keys());
    // see this blog post for details on why we're setting allowConcurrency:
    // https://blog.cloudflare.com/durable-objects-easy-fast-correct-choose-three/
    let getOptions: DurableObjectGetOptions = { allowConcurrency: true };
    let messageList = this.storage.get(keys, getOptions);
    if (DEBUG) {
      console.log("getRecentMessages() messageList:")
      console.log(await messageList)
    }
    return messageList;
  }

  setupSession(session: SessionType, msg: any) {
    let webSocket = session.webSocket;
    try {
      // The first message the client sends is the user info message with their pubKey. Save it
      // into their session object and in the visitor list.
      if (!this.#channelKeys) {
        webSocket.close(4000, "This room does not have an owner, or the owner has not enabled it. You cannot interact with it yet.");
        console.log("no owner - closing")
      }
      const data = jsonParseWrapper(msg.data.toString(), 'L733');
      if (data.pem) {
        webSocket.send(JSON.stringify({ error: "ERROR: PEM formats no longer used" }));
        return;
      }
      if (!data.name) {
        webSocket.send(JSON.stringify({ error: "ERROR: First message needs to contain pubKey" }));
        return;
      }
      let _name: JsonWebKey = jsonParseWrapper(data.name, 'L578');
      const isPreviousVisitor = sbCrypto.lookupKey(_name, this.visitors) >= 0;
      const isAccepted = sbCrypto.lookupKey(_name, this.accepted_requests) >= 0;
      if (!isPreviousVisitor && this.visitors.length >= this.room_capacity) {
        webSocket.close(4000, 'ERROR: The room is not accepting any more visitors.');
        return;
      }
      if (!isPreviousVisitor) {
        this.visitors.push(jsonParseWrapper(data.name, 'L594'));
        this.storage.put('visitors', JSON.stringify(this.visitors))
      }
      if (this.locked) {
        if (!isAccepted && !isPreviousVisitor) {
          this.join_requests.push(data.name);
          this.storage.put('join_requests', JSON.stringify(this.join_requests));
        } else {
          // TODO: this mechanism needs testing
          const encrypted_key = this.lockedKeys[sbCrypto.lookupKey(_name, this.lockedKeys)];
          this.#channelKeys!.lockedKey = encrypted_key;
        }

      }
      session.name = data.name;
      webSocket.send(JSON.stringify({ ready: true, keys: this.#channelKeys, motd: this.motd, roomLocked: this.locked }));

      session.room_id = "" + data.room_id;

      // Note that we've now received the user info message for this session
      session.receivedUserInfo = true;
    } catch (err: any) {
      webSocket.send(JSON.stringify({ error: "ERROR: problem setting up session: " + err.message + '\n' + err.stack }));
      return;
    }
  }

  async handleSession(webSocket: WebSocket, _ip: string | null) {
    webSocket.accept();
    // We don't send any messages to the client until it has sent us 
    // the initial user info (message which would be the client's pubKey)

    // Create our session and add it to the sessions list.
    let session: SessionType = {
      name: '',
      room_id: '',
      webSocket: webSocket,
      blockedMessages: await this.getRecentMessages(100),
      quit: false, // tracks cleanup, true means go away
      receivedUserInfo: false,
    };

    // track active connections
    this.sessions.push(session);

    webSocket.addEventListener("message", async msg => {
      try {
        if (session.quit) {
          webSocket.close(1011, "WebSocket broken (got a quit).");
          return;
        }
        if (!session.receivedUserInfo) {
          this.setupSession(session, msg);
          return;
        }
        if (jsonParseWrapper(msg.data.toString(), 'L788').ready) {
          // the client sends a "ready" message when it can start receiving
          if (!session.blockedMessages) return;
          // Deliver all the messages we queued up since the user connected.
          webSocket.send(JSON.stringify(session.blockedMessages))
          session.blockedMessages.clear()
          return;
        }

        // convenience for owner to know what it's seen
        this.ownerUnread += 1;

        // This part is important. Time stamps are monotonically increasing, but if two messages
        // arrive simultaneously, they'll have the same timestamp. To avoid this, we always set
        // the timestamp to be at least one millisecond greater than the last timestamp we saw.
        // We store it as an integer in lastTimestamp, but in the message it is encoded as up
        // to a 42-bit string of 0s and 1s (which allows efficient prefix search). This format
        // allows timestamps up to some time in September, 2248, by which time we will be
        // counting from the founding date of our Mars escape colony anyway.
        let tsNum = Math.max(Date.now(), this.lastTimestamp + 1);
        this.lastTimestamp = tsNum;
        this.storage.put('lastTimestamp', tsNum)
        let ts = tsNum.toString(2).padStart(42, "0");

        // appending timestamp to channel id.
        const key = this.room_id + ts;

        let _x: Dictionary<string> = {}
        _x[key] = jsonParseWrapper(msg.data.toString(), 'L812');
        await this.broadcast(JSON.stringify(_x))
        await this.storage.put(key, msg.data);

        // all messages are also stored in GLOBAL KV
        await this.env.MESSAGES_NAMESPACE.put(key, msg.data);
      } catch (error: any) {
        // Report any exceptions directly back to the client
        let err_msg = '[handleSession()] ' + error.message + '\n' + error.stack + '\n';
        console.log(err_msg);
        try {
          webSocket.send(JSON.stringify({ error: err_msg }));
        } catch {
          console.log("(NOTE - getting error on sending error message back to client)");
        }
      }
    });

    // On "close" and "error" events, remove matching sessions, and broadcast a quit
    let closeOrErrorHandler = () => {
      session.quit = true; // tells any closure to go away
      this.sessions = this.sessions.filter(member => member !== session);
    };
    webSocket.addEventListener("close", closeOrErrorHandler);
    webSocket.addEventListener("error", closeOrErrorHandler);
  }

  // broadcast() broadcasts a message to all clients.
  async broadcast(message: any) {
    if (typeof message !== "string")
      message = JSON.stringify(message);
    if (DEBUG) console.log("calling sendWebNotifications()", message);
    await this.sendWebNotifications(message);
    // Iterate over all the sessions sending them messages.
    this.sessions = this.sessions.filter(session => {
      if (session.name) {
        try {
          session.webSocket.send(message);
          if (session.name === this.#channelKeys?.ownerPubKeyX)
            this.ownerUnread -= 1;
          return true;
        } catch (err) {
          session.quit = true;
          return false; // delete session
        }
      } else {
        // This session hasn't sent the initial user info message yet, so we're not sending them
        // messages yet (no secret lurking!). Queue the message to be sent later.
        session.blockedMessages.push(message);
        return true;
      }
    });
    this.storage.put('ownerUnread', this.ownerUnread);
  }

  async handleOldMessages(request: Request) {
    try {
      const { searchParams } = new URL(request.url);
      const currentMessagesLength = Number(searchParams.get('currentMessagesLength')) || 100;
      const cursor = searchParams.get('cursor') || '';
      return returnResult(request, JSON.stringify(this.getRecentMessages(currentMessagesLength, cursor)), 200);
    } catch (error: any) {
      return returnError(request, "Could not fetch older messages: " + error.message, 500)
    }
  }

  async getKey(type: string): Promise<string | null> {
    if (this.personalRoom) {
      // keys managed by owner
      if (type === 'ledgerKey') return this.env.LEDGER_KEY;
      const ret = await this.storage.get(type);
      if (ret) return (ret as string)
      else return null;
    }
    // otherwise it's keys managed by SSO / server
    if (type === 'ownerKey') {
      let _keys_id = (await this.env.KEYS_NAMESPACE.list({ prefix: this.room_id + '_ownerKey' })).keys.map(key => key.name);
      if (_keys_id.length == 0) return null;
      let keys = _keys_id.map(async key => await this.env.KEYS_NAMESPACE.get(key));
      return await keys[keys.length - 1];
    } else if (type === 'ledgerKey') {
      return await this.env.KEYS_NAMESPACE.get(type);
    }
    return await this.env.KEYS_NAMESPACE.get(this.room_id + '_' + type);
  }

  // deprecated (and this was flawed), see notes in jslib
  async postPubKey(request: Request) {
    return returnError(request, "postPubKey is deprecated", 400)
  }

  async handleRoomCapacityChange(request: Request) {
    const { searchParams } = new URL(request.url);
    const newLimit = searchParams.get('capacity');
    this.room_capacity = Number(newLimit) || this.room_capacity;
    this.storage.put('room_capacity', this.room_capacity)
    return returnResult(request, JSON.stringify({ capacity: newLimit }), 200);
  }

  async getRoomCapacity(request: Request) {
    return returnResult(request, JSON.stringify({ capacity: this.room_capacity }), 200);
  }

  async getOwnerUnreadMessages(request: Request) {
    return returnResult(request, JSON.stringify({ unreadMessages: this.ownerUnread }), 200);
  }

  async getPubKeys(request: Request) {
    return returnResult(request, JSON.stringify({ keys: this.visitors }), 200);
  }

  async acceptVisitor(request: Request) {
    let data = await request.json();
    const acceptPubKey: JsonWebKey = jsonParseWrapper((data as any).pubKey, 'L783');
    // const ind = this.join_requests.indexOf((data as any).pubKey as string);
    const ind = sbCrypto.lookupKey(acceptPubKey, this.join_requests);
    if (ind >= 0) {
      this.accepted_requests = [...this.accepted_requests, ...this.join_requests.splice(ind, 1)];
      this.lockedKeys[(data as any).pubKey] = (data as any).lockedKey;
      this.storage.put('accepted_requests', JSON.stringify(this.accepted_requests));
      this.storage.put('lockedKeys', this.lockedKeys);
      this.storage.put('join_requests', JSON.stringify(this.join_requests))
      return returnResult(request, JSON.stringify({}), 200);
    } else {
      return returnError(request, "Could not accept visitor (visitor not found)", 400)
    }
  }

  async lockRoom(request: Request) {
    this.locked = true;
    for (let i = 0; i < this.visitors.length; i++) {
      if (this.accepted_requests.indexOf(this.visitors[i]) < 0 && this.join_requests.indexOf(this.visitors[i]) < 0) {
        this.join_requests.push(this.visitors[i]);
      }
    }
    this.storage.put('join_requests', JSON.stringify(this.join_requests));
    this.storage.put('locked', this.locked)
    return returnResult(request, JSON.stringify({ locked: this.locked }), 200);
  }

  async getJoinRequests(request: Request) {
    return returnResult(request, JSON.stringify({ join_requests: this.join_requests }), 200);
  }

  async isRoomLocked(request: Request) {
    return returnResult(request, JSON.stringify({ locked: this.locked }), 200);
  }

  async setMOTD(request: Request) {
    let data = await request.json();
    this.motd = (data as any).motd;
    this.storage.put('motd', this.motd);
    return returnResult(request, JSON.stringify({ motd: this.motd }), 200);
  }

  // TODO: we do not allow owner key rotations, but we need to add
  // regular key rotation(s), so keeping this as template code
  async ownerKeyRotation(request: Request) {
    if (ALLOW_OWNER_KEY_ROTATION) {
      let _tries = 3;
      let _timeout = 10000;
      let _success = await this.checkRotation(1);
      if (!_success) {
        while (_tries > 0) {
          _tries -= 1;
          _success = await this.checkRotation(_timeout)
          if (_success) {
            break;
          }
          _timeout *= 2;
        }
        if (!_success) {
          return returnResult(request, JSON.stringify({ success: false }), 200);
        }
      }
      const _updatedKey = await this.getKey('ownerKey');
      // ehm .. why was this removed?
      this.broadcast(JSON.stringify({ control: true, ownerKeyChanged: true, ownerKey: _updatedKey }));
      this.room_owner = _updatedKey;

      // Now pushing all accepted requests back to join requests
      this.join_requests = [...this.join_requests, ...this.accepted_requests];
      this.accepted_requests = [];
      this.lockedKeys = [];
      this.storage.put('join_requests', JSON.stringify(this.join_requests))
      this.storage.put('lockedKeys', JSON.stringify(this.lockedKeys))
      this.storage.put('accepted_requests', JSON.stringify(this.accepted_requests));
      return returnResult(request, JSON.stringify({ success: true }), 200);
    } else {
      return returnError(request, "Owner key rotation not allowed", 405);
    }
  }

  // clumsy event handling to track change; TODO cleanup
  async checkRotation(_timeout: number) {
    await new Promise(resolve => setTimeout(resolve, _timeout));
    /* return new Promise((resolve) => {
      setTimeout(async () => {
        if (await this.getKey('ownerKey') !== this.room_owner) {
          resolve(true);
        }
        resolve(false);
      }, _timeout)
    })*/
    return await this.getKey('ownerKey') !== this.room_owner;
  }

  // NOTE: current design limits this to 2^52 bytes, future limit will be 2^64 bytes
  roundSize(size: number, roundUp = true) {
    size = Number(size)
    if (size === Infinity) return Infinity; // special case
    if (size <= STORAGE_SIZE_MIN) size = STORAGE_SIZE_MIN;
    if (size > (2 ** 52)) throw new Error(`Storage size too large (max 2^52 and we got ${size})`);
    const exp1 = Math.floor(Math.log2(size));
    const exp2 = exp1 - 3;
    const frac = Math.floor(size / (2 ** exp2));
    const result = frac << exp2;
    if ((size > result) && roundUp) return result + (2 ** exp2);
    else return result;
  }

  /**
   * channels approve storage by creating storage token out of their budget
   */
  async handleNewStorage(request: Request) {
    const { searchParams } = new URL(request.url);
    const size = this.roundSize(Number(searchParams.get('size')));
    const storageLimit = this.storageLimit;
    if (size > storageLimit) return returnResult(request, JSON.stringify({ error: 'Not sufficient storage budget left in channel' }), 500);
    if (size > STORAGE_SIZE_MAX) return returnResult(request, JSON.stringify({ error: `Storage size too large (max ${STORAGE_SIZE_MAX} bytes)` }), 500);
    this.storageLimit = storageLimit - size;
    this.storage.put('storageLimit', this.storageLimit);
    const token_buffer = crypto.getRandomValues(new Uint8Array(48)).buffer;
    const token_hash_buffer = await crypto.subtle.digest('SHA-256', token_buffer)
    const token_hash = arrayBufferToBase64(token_hash_buffer);
    const kv_data = { used: false, size: size };
    const kv_resp = await this.env.LEDGER_NAMESPACE.put(token_hash, JSON.stringify(kv_data));
    const encrypted_token_id = arrayBufferToBase64(await crypto.subtle.encrypt({ name: "RSA-OAEP" }, this.ledgerKey!, token_buffer));
    const hashed_room_id = arrayBufferToBase64(await crypto.subtle.digest('SHA-256', (new TextEncoder).encode(this.room_id)));
    const token = { token_hash: token_hash, hashed_room_id: hashed_room_id, encrypted_token_id: encrypted_token_id };
    return returnResult(request, JSON.stringify(token), 200);
  }

  async getStorageLimit(request: Request) {
    return returnResult(request, JSON.stringify({ storageLimit: this.storageLimit }), 200);
  }

  async getMother(request: Request) {
    return returnResult(request, JSON.stringify({ motherChannel: this.motherChannel }), 200);
  }

  transferBudgetLegal(transferBudget: number) {
    // first check if the transfer budget is a number
    if (isNaN(transferBudget)) return false;
    // then check if it's within the allowed range
    return ((transferBudget > 0) && (transferBudget <= MAX_BUDGET_TRANSFER));
  }

  /*
     Transfer storage budget from one channel to another. Use the target
     channel's budget, and just get the channel ID from the request
     and look up and increment it's budget.
  */
  async handleBuddRequest(request: Request): Promise<Response> {
    if (DEBUG2) console.log(request)
    const _secret = this.env.SERVER_SECRET;
    const { searchParams } = new URL(request.url);
    const targetChannel = searchParams.get('targetChannel');
    let transferBudget = this.roundSize(Number(searchParams.get('transferBudget')));
    const serverSecret = searchParams.get('serverSecret');

    if (!targetChannel) return returnResult(request, JSON.stringify({ error: '[budd()]: No target channel specified' }), 500, 50);

    if (this.room_id === targetChannel) {
      // we are the benefactor
      const size = transferBudget;
      if (!this.transferBudgetLegal(size)) return returnResult(request, JSON.stringify({ error: `[budd()]: Transfer budget not legal (${size})` }), 500, 50);
      if ((serverSecret) && (serverSecret === _secret)) {
        // we are the target channel, so we're being asked to accept budgeta
        const roomInitialized = !(this.room_owner === "" || this.room_owner === null);
        if (roomInitialized) {
          // simple case, all is lined up and approved, and we've been initialized
          const newStorageLimit = this.storageLimit + size;
          this.storageLimit = newStorageLimit;
          await this.storage.put('storageLimit', newStorageLimit);
          if (DEBUG)
            console.log(`[budd()]: Transferring ${size} bytes from ${this.room_id.slice(0, 12)}... to ${targetChannel.slice(0, 12)}... (new recipient storage limit: ${newStorageLimit})`);
          returnResult(request, JSON.stringify({ success: true }), 200)
        } else {
          // we don't exist yet, so we convert this call to a self-approved 'upload'
          console.log("================ ARE WE EVER CALLED?? ================")
          // let data = await request.arrayBuffer();
          // let jsonString = new TextDecoder().decode(data);
          // let jsonData = jsonParseWrapper(jsonString, 'L937');
          // if (jsonData.hasOwnProperty("SERVER_SECRET")) return returnResult(request, JSON.stringify({ error: `[budd()]: SERVER_SECRET set? Huh?` }), 500, 50);
          // jsonData["SERVER_SECRET"] = _secret;
          // if (size < NEW_CHANNEL_MINIMUM_BUDGET)
          //   return returnResult(request, JSON.stringify({ error: `Not enough storage request for a new channel (minimum is ${this.NEW_CHANNEL_MINIMUM_BUDGET} bytes)` }), 500);
          // jsonData["size"] = size;
          // let newRequest = new Request(request.url, {
          //   method: 'POST',
          //   headers: request.headers,
          //   body: JSON.stringify(jsonData)
          // });
          // if (DEBUG) {
          //   console.log("[budd()]: Converting request to upload request:");
          //   console.log(newRequest);
          // }
          // await this.env.LEDGER_NAMESPACE.put(targetChannel, JSON.stringify(size));
          // if (DEBUG) { 
          //   console.log('++++ putting budget in ledger ... reading back:');
          //   console.log(await this.env.LEDGER_NAMESPACE.get(targetChannel));
          // }
          // return callDurableObject(targetChannel, 'uploadRoom', newRequest, this.env)
          return returnResult(request, JSON.stringify({ error: '[handleBuddRequest()] problems (L994)' }), 500)

        }
      } else {
        return returnResult(request, JSON.stringify({ error: '[budd()]: Authentication of transfer failed' }), 401, 50);
      }
    } else {
      // it's another channel, taken out of ours
      if (!this.storageLimit) return returnResult(request, JSON.stringify({ error: `[budd()]: Mother channel (${this.room_id.slice(0, 12)}...) either does not exist, or has not been initialized, or lacks storage budget` }), 500, 50);
      if ((!transferBudget) || (transferBudget === Infinity)) transferBudget = this.storageLimit; // strip it
      if (transferBudget > this.storageLimit) return returnResult(request, JSON.stringify({ error: '[budd()]: Not enough storage budget in mother channel for request' }), 500, 50);
      const size = transferBudget
      const newStorageLimit = this.storageLimit - size;
      this.storageLimit = newStorageLimit;
      await this.storage.put('storageLimit', newStorageLimit);

      if (DEBUG) console.log(`[budd()]: Removing ${size} bytes from ${this.room_id.slice(0, 12)}... and forwarding to ${targetChannel.slice(0, 12)}... (new mother storage limit: ${newStorageLimit} bytes)`);
      // await this.env.LEDGER_NAMESPACE.put(targetChannel, JSON.stringify(size));
      // if (DEBUG) { 
      //   console.log('++++ putting budget in ledger ... reading back:');
      //   console.log(await this.env.LEDGER_NAMESPACE.get(targetChannel));
      // }

      let data = await request.arrayBuffer();
      let jsonString = new TextDecoder().decode(data);
      let jsonData = jsonParseWrapper(jsonString, 'L1018');
      if (jsonData.hasOwnProperty("SERVER_SECRET")) return returnResult(request, JSON.stringify({ error: `[budd()]: SERVER_SECRET set? Huh?` }), 500, 50);
      jsonData["SERVER_SECRET"] = _secret;
      if (size < NEW_CHANNEL_MINIMUM_BUDGET)
        return returnResult(request, JSON.stringify({ error: `Not enough storage request for a new channel (minimum is ${NEW_CHANNEL_MINIMUM_BUDGET} bytes)` }), 500);
      jsonData["size"] = size;
      jsonData["motherChannel"] = this.room_id; // we leave a birth certificate behind
      let newUrl = new URL(request.url);
      newUrl.pathname = `/api/room/${targetChannel}/uploadRoom`;
      let newRequest = new Request(newUrl, {
        method: 'POST',
        body: JSON.stringify(jsonData),
        headers: {
          'Content-Type': 'application/json'
        }
      });
      if (DEBUG) {
        console.log("[budd()]: Converting request to upload request:");
        if (DEBUG2) console.log(newRequest);
      }
      await this.env.LEDGER_NAMESPACE.put(targetChannel, JSON.stringify({ mother: this.room_id, size: size }));
      if (DEBUG) {
        console.log('++++ putting budget in ledger ... reading back:');
        console.log(await this.env.LEDGER_NAMESPACE.get(targetChannel));
      }
      return callDurableObject(targetChannel, ['uploadRoom'], newRequest, this.env)

      // return callDurableObject(targetChannel, [ `budd?targetChannel=${targetChannel}&transferBudget=${size}&serverSecret=${_secret}` ], request, this.env);
    }
    return returnError(request, 'Should not get here', 500)
  }

  async handleAdminDataRequest(request: Request) {
    const adminData: ChannelAdminData = {
      room_id: this.room_id,
      join_requests: this.join_requests,
      capacity: this.room_capacity,
    }
    return returnResult(request, JSON.stringify(adminData), 200);
  }

  async verifySign(secretKey: CryptoKey, sign: any, contents: string) {
    const _sign = base64ToArrayBuffer(decodeURIComponent(sign));
    const encoder = new TextEncoder();
    const encoded = encoder.encode(contents);
    let verified = await crypto.subtle.verify(
      { name: 'ECDSA', hash: 'SHA-256' },
      secretKey,
      _sign,
      encoded
    );
    return verified;
  }

  async verifyCookie(request: Request) {
    let cookies: any = {};
    request.headers.has('cookie') && request.headers.get('cookie')!.split(';').forEach(function (cookie) {
      let parts = cookie.match(/(.*?)=(.*)$/)
      if (parts)
        cookies[parts[1].trim()] = (parts[2] || '').trim();
    });
    if (!cookies.hasOwnProperty('token_' + this.room_id)) {
      return false;
    }
    const verificationKey = await crypto.subtle.importKey("jwk", jsonParseWrapper(await this.env.KEYS_NAMESPACE.get(this.room_id + '_authorizationKey'), 'L778'), {
      name: "ECDSA",
      namedCurve: "P-384"
    }, false, ['verify']);
    const auth_parts = cookies['token_' + this.room_id].split('.');
    const payload = auth_parts[0];
    const sign = auth_parts[1];
    return (await this.verifySign(verificationKey, sign, payload + '_' + this.room_id) && ((new Date()).getTime() - parseInt(payload)) < 86400000);
  }

  // this checks if OWNER has signed the request
  async verifyAuthSign(request: Request): Promise<boolean> {
    if (!this.#channelKeys)
      return false;
    if (!request.headers.has('authorization'))
      return false;
    let authHeader = request.headers.get('authorization');
    if (!authHeader) return false
    let auth_parts = authHeader.split('.');
    if (new Date().getTime() - parseInt(auth_parts[0]) > 60000)
      return false;
    let sign = auth_parts[1];
    let ownerKey = this.#channelKeys!.ownerKey
    let roomSignKey = this.#channelKeys!.signKey
    let verificationKey = await crypto.subtle.deriveKey(
      {
        name: "ECDH",
        public: ownerKey
      },
      roomSignKey,
      {
        name: "HMAC",
        hash: "SHA-256",
        length: 256
      },
      false,
      ["verify"]);
    return await crypto.subtle.verify("HMAC", verificationKey, base64ToArrayBuffer(sign), new TextEncoder().encode(auth_parts[0]));
  }

  // returns true if request is either from OWNER, or with a signature cookie (eg SSO)
  async verifyAuth(request: Request): Promise<boolean> {
    return (await this.verifyCookie(request) || await this.verifyAuthSign(request))
  }

  registerDevice(request: Request) {
    return returnError(request, "registerDevice is disabled, use web notifications", 400)
  }

  async sendWebNotifications(message: string) {
    if (DEBUG) console.log("Sending web notification", message)
    message = JSON.parse(message)
    // if (message?.type === 'ack') return
    var coeff = 1000 * 60 * 1;
    var date = new Date();
    var rounded = new Date(Math.round(date.getTime() / coeff) * coeff)
    try {
      const options = {
        method: "POST",
        body: JSON.stringify({
          "channel_id": this.room_id,
          "notification": {
            silent: false,
            // replace notification in the queue, this limits message spam. 
            // We are limited in how we want to deliever notifications because of the encryption of messages
            // We limit the number of notifications to 1 per minute
            tag: `${this.room_id}${rounded}`,
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
      return await this.env.notifications.fetch("https://notifications.384.dev/notify", options)
    } catch (err) {
      console.log(err)
      console.log("Error sending web notification")
      return err
    }
  }

  async downloadAllData(request: Request) {
    let data: any = {
      roomId: this.room_id,
      ownerKey: this.room_owner,
      channelKeys: this.#channelKeys,
      guestKey: this.verified_guest,
      locked: this.locked,
      motd: this.motd,
    };
    if (await this.verifyAuth(request)) {
      data.adminData = { join_requests: this.join_requests, capacity: this.room_capacity };
      data.storageLimit = this.storageLimit;
      data.accepted_requests = this.accepted_requests;
      data.lockedKeys = this.lockedKeys;
      data.motherChannel = this.motherChannel;
      data.pubKeys = this.visitors;
      data.roomCapacity = this.room_capacity;
    }
    let dataBlob = new TextEncoder().encode(JSON.stringify(data));
    return returnResult(request, dataBlob, 200);
  }

  // used to create channels (from scratch) (or upload from backup)
  // TODO: can this overwrite a channel on the server?  is that ok?  (even if it's the owner)
  async uploadData(request: Request) {
    if (DEBUG) { console.log("== uploadData() =="); if (DEBUG2) console.log(request); }
    let _secret = this.env.SERVER_SECRET;
    let data = await request.arrayBuffer();
    let jsonString = new TextDecoder().decode(data);
    let jsonData = jsonParseWrapper(jsonString, 'L1416');
    let roomInitialized = this.room_owner != null;
    let requestAuthorized = jsonData.hasOwnProperty("SERVER_SECRET") && jsonData["SERVER_SECRET"] === _secret;
    let allowed = (roomInitialized && this.room_owner === jsonData["roomOwner"]) || requestAuthorized
    if (allowed) {
      if (DEBUG) console.log("uploadData() allowed")
      for (let key in jsonData) {
        // 2023.05.03: added filter to what properties are propagated (interface ChannelData)
        //             eg previous problem was it happily added SERVER_SECRET to DO storage
        if (DEBUG2) console.log("uploadData() key: ", key, "value: ", jsonData[key])
        if (["roomId", "channelId", "ownerKey", "encryptionKey", "signKey", "motherChannel"].includes(key))
          await this.storage.put(key, jsonData[key]);
        else
          if (DEBUG) console.log("**** uploadData() key not allowed: ", key)
      }
      this.personalRoom = true;
      this.storage.put("personalRoom", 'true');
      // if 'size' is provided in request, and request is authorized, set storageLimit
      if (jsonData.hasOwnProperty("size")) {
        let size = jsonData["size"];
        await this.storage.put("storageLimit", size);
      }
      // note that for a new room, "initialize" will fetch data from "this.storage" into object
      this.initialize(this.room_id)
      return returnResult(request, JSON.stringify({ success: true }), 200);
    } else {
      return returnResult(request, JSON.stringify({
        success: false,
        error: !roomInitialized ? "Server secret did not match" : "Room owner needs to upload the room"
      }), 500);
    }
  }

  async authorizeRoom(request: Request) {
    let _secret = this.env.SERVER_SECRET;
    let jsonData: any = await request.json();
    let requestAuthorized = jsonData.hasOwnProperty("SERVER_SECRET") && jsonData["SERVER_SECRET"] === _secret;
    if (requestAuthorized) {
      for (let key in jsonData) {
        await this.storage.put("room_owner", jsonData["ownerKey"]);
      }
      this.personalRoom = true;
      this.storage.put("personalRoom", 'true');
      this.room_owner = jsonData["room_owner"];
      return returnResult(request, JSON.stringify({ success: true }), 200);
    } else {
      return returnResult(request, JSON.stringify({ success: false, error: "Server secret did not match" }), 500);
    }
  }
}
