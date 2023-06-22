/// <reference types="@cloudflare/workers-types" />

/*
   Copyright (C) 2019-2023 Magnusson Institute, All Rights Reserved
   Contains code Copyright (C) 2022-2023 384, Inc.

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

const DEBUG = true;
const DEBUG2 = false;

if (DEBUG) console.log("++++ channel server code loaded ++++ DEBUG is enabled ++++")
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

import type { ChannelKeys, SBChannelId, ChannelAdminData, ChannelKeyStrings } from 'snackabra';
import { arrayBufferToBase64, base64ToArrayBuffer, jsonParseWrapper, SBCrypto, _sb_assert } from 'snackabra';
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
// 413: Payload Too Large
// 418: I'm a teapot
// 429: Too Many Requests
// 500: Internal Server Error
// 501: Not Implemented
// 507: Insufficient Storage (WebDAV/RFC4918)
//
type ResponseCode = 101 | 200 | 400 | 401 | 403 | 404 | 405 | 413 | 418 | 429 | 500 | 501 | 507;

function returnResult(request: Request, contents: any, status: ResponseCode, delay = 0) {
  const corsHeaders = {
    "Access-Control-Allow-Methods": "POST, OPTIONS, GET",
    "Access-Control-Allow-Headers": "Content-Type, authorization",
    "Access-Control-Allow-Credentials": "true",
    "Access-Control-Allow-Origin": request.headers.get("Origin") ?? "*",
    "Content-Type": "application/json;",
  }
  if (DEBUG2) console.log('++++++++++++HEADERS+++++++++++++\n\n', corsHeaders)
  return new Promise<Response>((resolve) => {
    setTimeout(() => {
      if (DEBUG2) console.log("++++ returnResult() contents:", contents, "status:", status)
      resolve(new Response(contents, { status: status, headers: corsHeaders }));
    }, delay);
  });
}

function returnError(_request: Request, errorString: string, status: ResponseCode, delay = 0) {
  if (DEBUG) console.log("**** ERROR: (status: " + status + ")\n" + errorString);
  if (!delay && ((status == 401) || (status == 403))) delay = 50; // delay if auth-related
  return returnResult(_request, `{ "error": "${errorString}" }`, status);
}

// this handles UNEXPECTED errors
async function handleErrors(request: Request, func: () => Promise<Response>) {
  try {
    return await func();
  } catch (err: any) {
    if (err instanceof Error) {
      if (request.headers.get("Upgrade") == "websocket") {
        const [_client, server] = Object.values(new WebSocketPair());
        if ((server as any).accept) {
          (server as any).accept(); // CF typing override (TODO: report this)
          server.send(JSON.stringify({ error: '[handleErrors()] ' + err.message + '\n' + err.stack }));
          server.close(1011, "Uncaught exception during session setup");
          console.log("webSocket close (error)")
        }
        return returnResult(request, null, 101);
      } else {
        return returnResult(request, err.stack, 500)
      }
    } else {
      return returnError(request, "Unknown error type (?) in top level", 500);
    }
  }
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
 *                                              note that locked rooms are not accessible until accepted   
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
    if (DEBUG) {
      console.log(`==== [${request.method}] Fetch called: ${request.url}`);
      if (DEBUG2) console.log(request.headers);
    }
    return await handleErrors(request, async () => {
      const url = new URL(request.url);
      const path = url.pathname.slice(1).split('/');
      if (request.method == "OPTIONS") {
        return returnResult(request, null, 200);
      }
      switch (path[0]) {
        case "api": // /api/... is only case currently
          return handleApiRequest(path.slice(1), request, env);
        default:
          return returnError(request, "Not found (must give API endpoint)", 404)
      }
    });
  }
}

// 'name' is room/channel name, 'path' is the rest of the path
async function callDurableObject(name: SBChannelId, path: Array<string>, request: Request, env: EnvType) {
  if (DEBUG) {
    console.log("==== callDurableObject() name:", name, "path:", path)
    if (DEBUG2) { console.log(request); console.log(env) }
  }
  // // SSO use case: fetch from another server to get pubkeys
  // const pubKey = await fetch("https://m063.dpn.workers.dev/api/v1/pubKeys?roomId=" + name)
  // if (pubKey?.error) return returnResult(request, JSON.stringify({ error: "Not found" }), 404);
  const roomId = env.channels.idFromName(name);
  const roomObject = env.channels.get(roomId);
  const newUrl = new URL(request.url);
  newUrl.pathname = "/" + name + "/" + path.join("/");
  const newRequest = new Request(newUrl.toString(), request);
  if (DEBUG2) { console.log("callDurableObject() newUrl:"); console.log(newUrl); }
  return roomObject.fetch(newRequest);
}

// 'path' is the request path, starting AFTER '/api'
async function handleApiRequest(path: Array<string>, request: Request, env: EnvType) {
  if (DEBUG) {
    console.log(`==== handleApiRequest() path:`);
    console.log(path);
    if (DEBUG2) console.log(request.headers);
  }
  try {
    switch (path[0]) {
      case "room":
      case "channel":
        return callDurableObject(path[1], path.slice(2), request, env);
      case "notifications":
        return returnError(request, "Device (Apple) notifications are disabled (use web notifications)", 400);
      case "getLastMessageTimes":
        {
          const _rooms: any = await request.json();
          const lastMessageTimes: Array<any> = [];
          for (let i = 0; i < _rooms.length; i++) {
            lastMessageTimes[_rooms[i]] = await lastTimeStamp(_rooms[i], env);
          }
          return returnResult(request, JSON.stringify(lastMessageTimes), 200);
        }
      default:
        return returnResult(request, JSON.stringify({ error: "Not found (this is an API endpoint, the URI was malformed)" }), 404)
    }
  } catch (error: any) {
    return returnError(request, `[API Call error] [${request.url}]: \n` + error.message + '\n' + error.stack, 500);
  }
}

async function lastTimeStamp(room_id: SBChannelId, env: EnvType) {
  let list_response = await env.MESSAGES_NAMESPACE.list({ "prefix": room_id });
  let message_keys: any = list_response.keys.map((res) => {
    return res.name
  });
  if (message_keys.length === 0) return '0'
  while (!list_response.list_complete) {
    list_response = await env.MESSAGES_NAMESPACE.list({ "cursor": list_response.cursor })
    message_keys = [...message_keys, list_response.keys];
  }
  return message_keys[message_keys.length - 1].slice(room_id.length);
}

interface Dictionary<T> {
  [index: string]: T;
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
  initializePromise: Promise<void> | null = null;
  sessions: Array<any> = [];
  #channelKeys?: ChannelKeys
  #channelKeyStrings?: ChannelKeyStrings
  room_id: SBChannelId = '';
  room_owner: string | null = null; // duplicate, placeholder / TODO cleanup=
  ownerCalls: ApiCallMap;
  visitorCalls: ApiCallMap;
  adminCalls: ApiCallMap;
  lastTimestamp: number = 0; // monotonically increasing timestamp
  storageLimit: number = 0;
  verified_guest: string = '';
  visitors: Array<JsonWebKey> = [];
  join_requests: Array<JsonWebKey> = [];
  accepted_requests: Array<JsonWebKey> = [];
  lockedKeys: Array<JsonWebKey> = []; // tracks history of lock keys
  room_capacity: number = 20;
  ownerUnread: number = 0;
  locked: boolean = false;
  motd: string = '';
  ledgerKey: CryptoKey | null = null;
  personalRoom: boolean = false;
  motherChannel: string = '';
  messagesCursor: string | null = null; // used for 'startAfter' option

  // DEBUG helper function to produce a string explainer of the current state
  #describe(): string {
    let s = 'CHANNEL STATE:\n';
    s += `room_id: ${this.room_id}\n`;
    s += `room_owner: ${this.room_owner}\n`;
    s += `verified_guest: ${this.verified_guest}\n`;
    s += `room_capacity: ${this.room_capacity}\n`;
    // s += `ownerUnread: ${this.ownerUnread}\n`;
    s += `locked: ${this.locked}\n`;
    // s += `motd: ${this.motd}\n`;
    s += `storageLimit: ${this.storageLimit}\n`;
    s += `personalRoom: ${this.personalRoom}\n`;
    s += `motherChannel: ${this.motherChannel}\n`;
    // s += `messagesCursor: ${this.messagesCursor}\n`;
    // s += `lastTimestamp: ${this.lastTimestamp}\n`;
    // s += `visitors: ${this.visitors}\n`;
    // s += `join_requests: ${this.join_requests}\n`;
    // s += `accepted_requests: ${this.accepted_requests}\n`;
    // s += `lockedKeys: ${this.lockedKeys}\n`;
    return s;
  }

  constructor(state: DurableObjectState, env: EnvType) {
    // NOTE: DO storage has a different API than global KV, see:
    // https://developers.cloudflare.com/workers/runtime-apis/durable-objects/#transactional-storage-api
    this.storage = state.storage;
    this.env = env;

    this.ownerCalls = {
      "/acceptVisitor": this.#acceptVisitor.bind(this),
      "/budd": this.#handleBuddRequest.bind(this),
      "/getAdminData": this.#handleAdminDataRequest.bind(this),
      "/getJoinRequests": this.#getJoinRequests.bind(this),
      "/getMother": this.#getMother.bind(this),
      "/getPubKeys": this.#getPubKeys.bind(this),
      "/getStorageLimit": this.#getStorageLimit.bind(this),
      "/lockRoom": this.#lockRoom.bind(this),
      "/motd": this.#setMOTD.bind(this),
      "/ownerKeyRotation": this.#ownerKeyRotation.bind(this), // deprecated
      "/ownerUnread": this.#getOwnerUnreadMessages.bind(this),
      "/updateRoomCapacity": this.#handleRoomCapacityChange.bind(this),
    }
    this.visitorCalls = {
      "/downloadData": this.#downloadAllData.bind(this),
      "/getChannelKeys": this.#getChannelKeys.bind(this),
      "/getRoomCapacity": this.#getRoomCapacity.bind(this), // TODO: this should be owner
      "/oldMessages": this.#handleOldMessages.bind(this),
      "/postPubKey": this.#postPubKey.bind(this), // deprecated
      "/registerDevice": this.#registerDevice.bind(this), // deprecated
      "/roomLocked": this.#isRoomLocked.bind(this),
      "/storageRequest": this.#handleNewStorage.bind(this),
    }
    this.adminCalls = {
      "/authorizeRoom": this.#authorizeRoom.bind(this),
      "/uploadRoom": this.#uploadData.bind(this)
    }
  }

  // need the initialize method to restore state of room when the worker is updated
  async #initialize(room_id: SBChannelId) {
    if (DEBUG) console.log(`==== ChannelServer.initialize() called for room: ${room_id} ====`)
    this.room_id = room_id;
    await this.storage.put('room_id', room_id); // in case we're new

    const ledgerKeyString = this.env.LEDGER_KEY;
    if (!ledgerKeyString)
      throw new Error("ERROR: no ledger key found in environment (fatal)");
    // ledger is RSA-OAEP so we do not use sbCrypto
    const ledgerKey = await crypto.subtle.importKey("jwk", jsonParseWrapper(ledgerKeyString, 'L217'), { name: "RSA-OAEP", hash: 'SHA-256' }, true, ["encrypt"])
    this.ledgerKey = ledgerKey; // a bit quicker

    // this is first, since #getKey() needs it
    this.personalRoom = (await this.#getKey('personalRoom')) == 'false' ? false : true;

    const keyStrings: ChannelKeyStrings = {
      ownerKey: await this.#getKey('ownerKey') || '',
      encryptionKey: await this.#getKey('encryptionKey') || '',
      signKey: await this.#getKey('signKey') || ''
    }
    if (DEBUG) console.log("keyStrings: ", keyStrings)

    // verify owner key viz room ID
    const ownerKeyJWK: JsonWebKey = jsonParseWrapper(keyStrings.ownerKey, 'L426')
    if (!(await sbCrypto.verifyChannelId(ownerKeyJWK, room_id))) {
      if (DEBUG) {
        console.log("ERROR: owner key does not match room ID (fatal)");
        console.log("ownerKey: ", keyStrings.ownerKey);
        console.log("generated room_id: ", await sbCrypto.generateChannelId(ownerKeyJWK));
        console.log("room_id: ", room_id);
      }
      throw new Error("ERROR: owner key does not match room ID (fatal)");
    }

    this.#channelKeyStrings = keyStrings;
    this.#channelKeys = await sbCrypto.channelKeyStringsToCryptoKeys(keyStrings)

    this.lastTimestamp = Number(await this.#getKey('lastTimestamp')) || 0;
    this.room_owner = await this.#getKey('ownerKey');
    this.verified_guest = await this.#getKey('guestKey') || '';
    const roomCapacity = await this.#getKey('room_capacity')
    this.room_capacity = roomCapacity === '0' ? 0 : Number(roomCapacity) || 20;
    this.visitors = jsonParseWrapper(await this.#getKey('visitors') || JSON.stringify([]), 'L220');
    this.ownerUnread = Number(await this.#getKey('ownerUnread')) || 0;
    this.locked = (await this.#getKey('locked')) === 'true' ? true : false;
    this.join_requests = jsonParseWrapper(await this.#getKey('join_requests') || JSON.stringify([]), 'L223');

    const storageLimit = Number(await this.#getKey('storageLimit'))
    if (storageLimit === Infinity) {
      // if there is no storageLimit, then this is a new room
      const ledgerData = await this.env.LEDGER_NAMESPACE.get(room_id);
      if (ledgerData) {
        // if there's a ledger entry then it's a budded room
        const { size, mother } = jsonParseWrapper(ledgerData, 'L311');
        // this.storageLimit = size
        this.storageLimit = 0 // this will actually be topped up in 'upload'
        if (DEBUG2) console.log(`note that size in ledger was ${size}, in case that differs from json`)
        this.motherChannel = mother
        if (DEBUG) console.log(`[initialize] Found storageLimit in ledger: ${this.storageLimit}`)
      } else {
        this.storageLimit = NEW_CHANNEL_BUDGET;
        this.motherChannel = 'BOOTSTRAP';
        if (DEBUG) console.log(`++++ new channel, no SIZE provided, setting to default (${this.storageLimit / (1024 * 1024)} MiB)`)
      }
      await this.storage.put('motherChannel', this.motherChannel);
      await this.storage.put('storageLimit', this.storageLimit);
    } else {
      this.storageLimit = storageLimit;
      this.motherChannel = await this.#getKey('motherChannel') || 'grandfathered';
    }

    this.accepted_requests = jsonParseWrapper(await this.#getKey('accepted_requests') || JSON.stringify([]), 'L224');
    this.lockedKeys = jsonParseWrapper(await this.#getKey('lockedKeys'), 'L467') || [];

    // TODO: test refactored lock
    // for (let i = 0; i < this.accepted_requests.length; i++)
    //   // this.lockedKeys[this.accepted_requests[i]] = await storage.get(this.accepted_requests[i]);
    //   this.lockedKeys[this.accepted_requests[i].x!] = await this.storage.get(this.accepted_requests[i]);

    this.motd = await this.#getKey('motd') || '';

    if (DEBUG) {
      console.log("Done creating room:")
      console.log("room_id: ", this.room_id)
      if (DEBUG2) console.log(this)
    }
  }

  async fetch(request: Request) {
    if (DEBUG) {
      console.log(`==== [Durable Object] fetch() called: ${request.url}`)
      if (DEBUG2) console.log(request.headers)
    }
    const url = new URL(request.url);
    const path = url.pathname.slice(1).split('/');
    if (DEBUG) console.log("path: ", path)

    // handle cases of either new channel, or reloaded object
    if (!this.room_id) {
      const roomId = await this.storage.get('room_id')
        .catch((error) => returnError(request, `ERROR: unable to fetch room_id ${error}`, 500));
      if (roomId) {
        // channel exists but object needs reloading
        if (roomId !== path[0]) {
          if (DEBUG) {
            console.log("**** room_id mismatch:");
            console.log("roomId: ", roomId);
            console.log("path[0]: ", path[0]);
          }
          return returnError(request, "ERROR: room_id mismatch (?)", 500);
        }
        this.room_id = roomId;
        await this.#initialize(roomId);
      } else if ((path) && (path[1] === 'uploadRoom')) {
        const ret = await this.#createChannel(request.clone());
        if (ret) return ret; // if there was an error, return it, otherwise fall through
      } else {
        // no channel, no object, no upload, no dice
        console.log(this.#describe())
        return returnError(request, "Not found (no channel) - only permitted first-touch is an authorized uploadRoom", 404);
      }
    }

    if (this.verified_guest === '') // TODO: this needed?
      this.verified_guest = await this.#getKey('guestKey') || '';

    if (this.room_id !== path[0])
      return returnError(request, "ERROR: room_id mismatch (?) [L522]", 500);

    return await handleErrors(request, async () => {

      // // SSO code - this would optionally verify public key from a public record room->key
      // if (this.room_owner === '') {
      //   this.room_id = url.pathname.split('/')[1];
      //   const keyFetch_json = await fetch("https://m063.dpn.workers.dev/api/v1/pubKeys?roomId=" + this.room_id)
      //   this.room_owner = JSON.stringify(keyFetch_json.ownerKey);
      //   this.encryptionKey = JSON.stringify(keyFetch_json.encryptionKey);
      // }

      // API section, all *synchronous* API calls are routed trough here
      const apiCall = '/' + path[1]
      try {
        if (apiCall === "/websocket") {
          if (request.headers.get("Upgrade") != "websocket")
            return returnError(request, "Expected websocket", 400);
          // note: if locked, verification is done in #handleSession()
          const ip = request.headers.get("CF-Connecting-IP");
          const pair = new WebSocketPair();
          await this.#handleSession(pair[1], ip);
          return new Response(null, { status: 101, webSocket: pair[0] });
        } else if (this.ownerCalls[apiCall]) {
          if (await this.#verifyAuth(request)) {
            return await this.ownerCalls[apiCall]!(request);
          } else {
            return returnError(request, "Owner verification failed (restricted API call)", 401);
          }
        } else if (this.visitorCalls[apiCall]) {
          // TODO: locked rooms should not be accessible until accepted
          // const data = jsonParseWrapper(msg.data.toString(), 'L733');
          // const _name: JsonWebKey = jsonParseWrapper(data.name, 'L578');
          // const isPreviousVisitor = sbCrypto.lookupKey(_name, this.visitors) >= 0;
          // const isAccepted = sbCrypto.lookupKey(_name, this.accepted_requests) >= 0;
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
  async #getRecentMessages(howMany: number, cursor = ''): Promise<Map<string, unknown>> {
    const listOptions: DurableObjectListOptions = { limit: howMany, prefix: this.room_id, reverse: true };
    if (cursor !== '')
      listOptions.startAfter = cursor;
    const keys = Array.from((await this.storage.list(listOptions)).keys());
    // see this blog post for details on why we're setting allowConcurrency:
    // https://blog.cloudflare.com/durable-objects-easy-fast-correct-choose-three/
    const getOptions: DurableObjectGetOptions = { allowConcurrency: true };
    const messageList = await this.storage.get(keys, getOptions);
    // we copy the Map we have in messageList, to a fresh map where each entry is converted with JSON.parse
    // because the parallel get above returns un-parsed objects (obviously)
    const messageMap = new Map<string, unknown>();
    for (const [key, value] of messageList.entries())
      messageMap.set(key, JSON.parse(value as string));
    // if (DEBUG) { console.log("getRecentMessages() messageList:"); console.log(messageMap) }
    return messageMap;
  }

  #setupSession(session: SessionType, msg: any) {
    const webSocket = session.webSocket;
    try {
      // The first message the client sends is the user info message with their pubKey.
      // Save it into their session object and in the visitor list.
      if (!this.#channelKeys) {
        webSocket.close(4000, "This room does not have an owner, or the owner has not enabled it. You cannot interact with it yet.");
        if (DEBUG) console.log("no owner - closing")
        return;
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
      const _name: JsonWebKey = jsonParseWrapper(data.name, 'L578');
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
          // TODO ok did we not reject before? when/where should we signal/enforce?
          webSocket.close(4000, "ERROR: this is a locked room and you haven't yet been accepted by owner.");
          return;
        } else {
          // TODO: this mechanism needs testing
          const encrypted_key = this.lockedKeys[sbCrypto.lookupKey(_name, this.lockedKeys)];
          this.#channelKeys!.lockedKey = encrypted_key;
        }
      }
      session.name = data.name;
      webSocket.send(JSON.stringify({
        ready: true,
        keys: {
          encryptionKey: this.#channelKeyStrings!.encryptionKey,
          ownerKey: this.#channelKeyStrings!.ownerKey,
          signKey: this.#channelKeyStrings!.signKey,
          // TODO: guest key?
        },
        motd: this.motd, roomLocked: this.locked
      }));
      session.room_id = "" + data.room_id;
      // Note that we've now received the user info message for this session
      session.receivedUserInfo = true;
    } catch (err: any) {
      webSocket.send(JSON.stringify({ error: "ERROR: problem setting up session: " + err.message + '\n' + err.stack }));
      return;
    }
  }

  async #handleSession(webSocket: WebSocket, _ip: string | null) {
    // per CF documentation, first accept() it; this conflicts with CF types
    if (!(webSocket as any).accept)
      // conservative coding viz typing override
      throw new Error("ERROR: webSocket does not have accept() method");
    (webSocket as any).accept(); // typing override
    // We don't send any messages to the client until it has sent us 
    // the initial user info (message which would be the client's pubKey)
    // Create our session and add it to the sessions list.
    const session: SessionType = {
      name: '',
      room_id: '',
      webSocket: webSocket,
      blockedMessages: await this.#getRecentMessages(100),
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
        if (!session.receivedUserInfo) {
          this.#setupSession(session, msg);
          return;
        }

        const msgData = jsonParseWrapper(msg.data.toString(), 'L692');

        console.log("------------ getting msg from client ------------")
        console.log(msg)
        console.log(msgData)
        console.log("-------------------------------------------------")

        if (msgData.ready) {
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
        const tsNum = Math.max(Date.now(), this.lastTimestamp + 1);
        this.lastTimestamp = tsNum;
        this.storage.put('lastTimestamp', tsNum)
        const ts = tsNum.toString(2).padStart(42, "0");

        // appending timestamp to channel id.
        const key = this.room_id + ts;

        // TODO: last use of Dictioary :-)
        const _x: Dictionary<string> = {}
        _x[key] = msgData;

        // We don't block on any of these: (ASYNC)

        // Here is the main workhorse ... actually send the message to every listener
        this.#broadcast(JSON.stringify(_x))

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
          console.log(`ERROR: was unable to propagate error to client: ${err_msg}`);
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
  }

  // broadcasts a message to all clients.
  async #broadcast(message: any) {
    if (typeof message !== "string")
      message = JSON.stringify(message);

    // TODO: we don't send notifications for everything? for example locked-out messages?
    if (DEBUG2) console.log("calling sendWebNotifications()", message);
    await this.#sendWebNotifications(message);

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

  async #handleOldMessages(request: Request) {
    const { searchParams } = new URL(request.url);
    const currentMessagesLength = Number(searchParams.get('currentMessagesLength')) || 100;
    const cursor = searchParams.get('cursor') || '';
    const messageMap = await this.#getRecentMessages(currentMessagesLength, cursor);
    let messageArray: { [key: string]: any } = {};
    for (let [key, value] of messageMap)
      messageArray[key] = value;
    return returnResult(request, JSON.stringify(messageArray), 200);
  }

  async #getKey(type: string): Promise<string | null> {
    if (this.personalRoom) {
      // keys managed by owner
      if (type === 'ledgerKey') return this.env.LEDGER_KEY;
      return await this.storage.get(type) || null;
    }
    // otherwise it's keys managed by SSO / server
    if (type === 'ownerKey') {
      const _keys_id = (await this.env.KEYS_NAMESPACE.list({ prefix: this.room_id + '_ownerKey' })).keys.map(key => key.name);
      if (_keys_id.length == 0) return null;
      const keys = _keys_id.map(async key => await this.env.KEYS_NAMESPACE.get(key));
      return await keys[keys.length - 1];
    } else if (type === 'ledgerKey') {
      return await this.env.KEYS_NAMESPACE.get(type);
    }
    return await this.env.KEYS_NAMESPACE.get(this.room_id + '_' + type);
  }

  // deprecated (and this was flawed), see notes in jslib
  async #postPubKey(request: Request) {
    return returnError(request, "postPubKey is deprecated", 400)
  }

  async #handleRoomCapacityChange(request: Request) {
    const { searchParams } = new URL(request.url);
    const newLimit = searchParams.get('capacity');
    this.room_capacity = Number(newLimit) || this.room_capacity;
    this.storage.put('room_capacity', this.room_capacity)
    return returnResult(request, JSON.stringify({ capacity: newLimit }), 200);
  }

  async #getRoomCapacity(request: Request) {
    return returnResult(request, JSON.stringify({ capacity: this.room_capacity }), 200);
  }

  async #getOwnerUnreadMessages(request: Request) {
    return returnResult(request, JSON.stringify({ unreadMessages: this.ownerUnread }), 200);
  }

  async #getPubKeys(request: Request) {
    return returnResult(request, JSON.stringify({ keys: this.visitors }), 200);
  }

  async #acceptVisitor(request: Request) {
    const data = await request.json();
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

  async #lockRoom(request: Request) {
    this.locked = true;
    for (let i = 0; i < this.visitors.length; i++)
      if (this.accepted_requests.indexOf(this.visitors[i]) < 0 && this.join_requests.indexOf(this.visitors[i]) < 0)
        this.join_requests.push(this.visitors[i]);
    this.storage.put('join_requests', JSON.stringify(this.join_requests));
    this.storage.put('locked', this.locked)
    return returnResult(request, JSON.stringify({ locked: this.locked }), 200);
  }

  async #getJoinRequests(request: Request) {
    return returnResult(request, JSON.stringify({ join_requests: this.join_requests }), 200);
  }

  async #isRoomLocked(request: Request) {
    return returnResult(request, JSON.stringify({ locked: this.locked }), 200);
  }

  async #setMOTD(request: Request) {
    const data = await request.json();
    this.motd = (data as any).motd;
    this.storage.put('motd', this.motd);
    return returnResult(request, JSON.stringify({ motd: this.motd }), 200);
  }

  // TODO: we do not allow owner key rotations, but we need to add
  // regular key rotation(s), so keeping this as template code
  async #ownerKeyRotation(request: Request) {
    if (ALLOW_OWNER_KEY_ROTATION) {
      let _tries = 3;
      let _timeout = 10000;
      let _success = await this.#checkRotation(1);
      if (!_success) {
        while (_tries > 0) {
          _tries -= 1;
          _success = await this.#checkRotation(_timeout)
          if (_success) {
            break;
          }
          _timeout *= 2;
        }
        if (!_success) {
          return returnResult(request, JSON.stringify({ success: false }), 200);
        }
      }
      // const _updatedKey = await this.getKey('ownerKey');
      const _updatedKey = this.room_owner;
      // ehm .. why was this removed?
      this.#broadcast(JSON.stringify({ control: true, ownerKeyChanged: true, ownerKey: _updatedKey }));
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
  async #checkRotation(_timeout: number): Promise<boolean> {
    await new Promise((resolve) => setTimeout(
      () => {
        resolve(true);
      }, _timeout));
    return (true)
  }

  // NOTE: current design limits this to 2^52 bytes, future limit will be 2^64 bytes
  #roundSize(size: number, roundUp = true) {
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

  // channels approve storage by creating storage token out of their budget
  async #handleNewStorage(request: Request) {
    const { searchParams } = new URL(request.url);
    const size = this.#roundSize(Number(searchParams.get('size')));
    const storageLimit = this.storageLimit;
    if (size > storageLimit) return returnError(request, 'Not sufficient storage budget left in channel', 507);
    if (size > STORAGE_SIZE_MAX) return returnResult(request, `Storage size too large (max ${STORAGE_SIZE_MAX} bytes)`, 413);
    this.storageLimit = storageLimit - size;
    this.storage.put('storageLimit', this.storageLimit);
    const token_buffer = crypto.getRandomValues(new Uint8Array(48)).buffer;
    const token_hash_buffer = await crypto.subtle.digest('SHA-256', token_buffer)
    const token_hash = arrayBufferToBase64(token_hash_buffer);
    const kv_data = { used: false, size: size };
    await this.env.LEDGER_NAMESPACE.put(token_hash, JSON.stringify(kv_data));
    const encrypted_token_id = arrayBufferToBase64(await crypto.subtle.encrypt({ name: "RSA-OAEP" }, this.ledgerKey!, token_buffer));
    const hashed_room_id = arrayBufferToBase64(await crypto.subtle.digest('SHA-256', (new TextEncoder).encode(this.room_id)));
    const token = { token_hash: token_hash, hashed_room_id: hashed_room_id, encrypted_token_id: encrypted_token_id };
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

     TODO: if the request amount is NEGATIVE, then that should be the
     amount of storage left behind in the mother channel
  */
  async #handleBuddRequest(request: Request): Promise<Response> {
    if (DEBUG2) console.log(request)
    const _secret = this.env.SERVER_SECRET;
    const { searchParams } = new URL(request.url);
    const targetChannel = searchParams.get('targetChannel');
    let transferBudget = this.#roundSize(Number(searchParams.get('transferBudget')));

    if (!targetChannel)
      return returnError(request, '[budd()]: No target channel specified', 400);
    if (this.room_id === targetChannel)
        return returnResult(request, JSON.stringify({ success: true }), 200); // no-op
    if (!this.storageLimit) {
      if (DEBUG) {
        console.log("storageLimit missing in mother channel?");
        console.log(this.#describe());
      }
      return returnError(request, `[budd()]: Mother channel (${this.room_id.slice(0, 12)}...) either does not exist, or has not been initialized, or lacks storage budget`, 400);
    }

    if ((!transferBudget) || (transferBudget === Infinity)) transferBudget = this.storageLimit; // strip it
    if (transferBudget > this.storageLimit) return returnError(request, '[budd()]: Not enough storage budget in mother channel for request', 507);
    const size = transferBudget
    const newStorageLimit = this.storageLimit - size;
    this.storageLimit = newStorageLimit;
    await this.storage.put('storageLimit', newStorageLimit);

    if (DEBUG) console.log(`[budd()]: Removing ${size} bytes from ${this.room_id.slice(0, 12)}... and forwarding to ${targetChannel.slice(0, 12)}... (new mother storage limit: ${newStorageLimit} bytes)`);

    const data = await request.arrayBuffer();
    const jsonString = new TextDecoder().decode(data);
    let jsonData = jsonString ? jsonParseWrapper(jsonString, 'L1018') : {};
    if (jsonData.hasOwnProperty("SERVER_SECRET")) return returnError(request, `[budd()]: SERVER_SECRET set? Huh?`, 403);

    jsonData["SERVER_SECRET"] = _secret; // we are authorizing this creation/transfer
    if (size < NEW_CHANNEL_MINIMUM_BUDGET)
      return returnError(request, `Not enough storage request for a new channel (minimum is ${NEW_CHANNEL_MINIMUM_BUDGET} bytes)`, 400);
    jsonData["size"] = size;
    jsonData["motherChannel"] = this.room_id; // we leave a birth certificate behind

    const newUrl = new URL(request.url);
    newUrl.pathname = `/api/room/${targetChannel}/uploadRoom`;
    const newRequest = new Request(newUrl.toString(), {
      method: 'POST',
      body: JSON.stringify(jsonData),
      headers: {
        'Content-Type': 'application/json'
      }
    });
    if (DEBUG) {
      console.log("[budd()]: Converting request to upload request");
      if (DEBUG2) console.log(newRequest);
    }
    // we block on ledger since this will be verified
    await this.env.LEDGER_NAMESPACE.put(targetChannel, JSON.stringify({ mother: this.room_id, size: size }));
    if (DEBUG) {
      console.log('++++ putting budget in ledger ... reading back:');
      console.log(await this.env.LEDGER_NAMESPACE.get(targetChannel));
    }
    return callDurableObject(targetChannel, ['uploadRoom'], newRequest, this.env)

    // return callDurableObject(targetChannel, [ `budd?targetChannel=${targetChannel}&transferBudget=${size}&serverSecret=${_secret}` ], request, this.env);
  }

  async #handleAdminDataRequest(request: Request) {
    const adminData: ChannelAdminData = {
      room_id: this.room_id,
      join_requests: this.join_requests,
      capacity: this.room_capacity,
    }
    return returnResult(request, JSON.stringify(adminData), 200);
  }

  async #verifySign(secretKey: CryptoKey, sign: any, contents: string) {
    const _sign = base64ToArrayBuffer(decodeURIComponent(sign));
    const encoder = new TextEncoder();
    const encoded = encoder.encode(contents);
    const verified = await crypto.subtle.verify(
      { name: 'ECDSA', hash: 'SHA-256' },
      secretKey,
      _sign,
      encoded
    );
    return verified;
  }

  async #verifyCookie(request: Request) {
    const cookies: any = {};
    request.headers.has('cookie') && request.headers.get('cookie')!.split(';').forEach(function (cookie) {
      const parts = cookie.match(/(.*?)=(.*)$/)
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
    return (await this.#verifySign(verificationKey, sign, payload + '_' + this.room_id) && ((new Date()).getTime() - parseInt(payload)) < 86400000);
  }

  // this checks if OWNER has signed the request
  async #verifyAuthSign(request: Request): Promise<boolean> {
    if (DEBUG) console.log("==== verifyAuthSign():")
    if (!this.#channelKeys) {
      if (DEBUG) console.log("verifyAuthSign(): no channel keys")
      return false;
    }
    const authHeader = request.headers.get('authorization');
    if (!authHeader) {
      if (DEBUG) {
        console.log("verifyAuthSign(): no authorization header")
        console.log("request.headers:")
        console.log(request.headers)
      }
      return false;
    }
    const auth_parts = authHeader.split('.');
    if (new Date().getTime() - parseInt(auth_parts[0]) > 60000) {
      if (DEBUG) console.log("verifyAuthSign(): auth token expired")
      return false;
    }
    const sign = auth_parts[1];
    const ownerKey = this.#channelKeys!.ownerKey
    const roomSignKey = this.#channelKeys!.signKey
    const verificationKey = await crypto.subtle.deriveKey(
      {
        name: "ECDH",
        public: ownerKey  // looks like possible issues with cloudflare worker types?
      },
      roomSignKey,
      {
        name: "HMAC",
        hash: "SHA-256",
        length: 256
      },
      false,
      ["verify"]);
    if (DEBUG2) {
      console.log("verifyAuthSign():\nsign (auth_parts[1]): ")
      console.log(sign)
      console.log("ownerKey: ")
      console.log(ownerKey)
      console.log("roomSignKey: ")
      console.log(roomSignKey)
      console.log("verificationKey: ")
      console.log(verificationKey)
      console.log("auth_parts[0]: ")
      console.log(auth_parts[0])
    }
    return await crypto.subtle.verify("HMAC", verificationKey, base64ToArrayBuffer(sign), new TextEncoder().encode(auth_parts[0]));
  }

  // returns true if request is either from OWNER, or with a signature cookie (eg SSO)
  async #verifyAuth(request: Request): Promise<boolean> {
    return (await this.#verifyCookie(request) || await this.#verifyAuthSign(request))
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
    message = JSON.parse(message)
    // if (message?.type === 'ack') return
    const coeff = 1000 * 60 * 1;
    const date = new Date();
    const rounded = new Date(Math.round(date.getTime() / coeff) * coeff)
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
      return await envNotifications.fetch("https://notifications.384.dev/notify", options)
    } catch (err) {
      console.log(err)
      console.log("Error sending web notification")
      return err
    }
  }

  async #downloadAllData(request: Request) {
    const data: any = {
      roomId: this.room_id,
      ownerKey: this.room_owner,
      channelKeys: this.#channelKeys,
      guestKey: this.verified_guest,
      locked: this.locked,
      motd: this.motd,
    };
    if (await this.#verifyAuth(request)) {
      data.adminData = { join_requests: this.join_requests, capacity: this.room_capacity };
      data.storageLimit = this.storageLimit;
      data.accepted_requests = this.accepted_requests;
      data.lockedKeys = this.lockedKeys;
      data.motherChannel = this.motherChannel;
      data.pubKeys = this.visitors;
      data.roomCapacity = this.room_capacity;
    }
    const dataBlob = new TextEncoder().encode(JSON.stringify(data));
    return returnResult(request, dataBlob, 200);
  }

  async #createChannel(request: Request): Promise<Response | null> {
    // request cloning is done by callee
    const jsonString = new TextDecoder().decode(await request.arrayBuffer());
    const jsonData = jsonParseWrapper(jsonString, 'L1128');
    const url = new URL(request.url);
    const path = url.pathname.slice(1).split('/');
    if (DEBUG) {
      console.log("==== createChannel(): jsonData: ")
      console.log(jsonData)
    }
    if (!(jsonData.hasOwnProperty("SERVER_SECRET") || jsonData["SERVER_SECRET"] === this.env.SERVER_SECRET))
      return returnError(request, "Not authorized to create channel", 401);
    const newOwnerKey = jsonData["ownerKey"];
    if (!newOwnerKey)
      return returnError(request, "No owner key provided", 400);
    this.room_owner = newOwnerKey;
    await this.storage.put("room_owner", newOwnerKey); // signals channel has been validly created
    const newOwnerKeyJson = jsonParseWrapper(newOwnerKey, 'L1218');
    if (!(await sbCrypto.verifyChannelId(newOwnerKeyJson, path[0]))) {
      if (DEBUG) {
        console.log("createChannel(): newOwnerKey: ", newOwnerKey)
        console.log("createChannel(): generated ID: ")
        console.log(await sbCrypto.generateChannelId(newOwnerKeyJson))
        console.log("createChannel(): path[0]: ")
        console.log(path[0])
      }
      return returnError(request, "Owner key does not match channel id (validation of channel viz keys failed)", 400);
    }
    for (const key of ["ownerKey", "encryptionKey", "signKey", "motherChannel", "visitors"]) {
      const newData = jsonData[key];
      if (newData) {
        if (DEBUG2) console.log("++ createChannel(): putting key, value: ", key, newData)
        await this.storage.put(key, newData);
      }
    }
    await this.storage.put("personalRoom", 'true');

    // signal a new room - this will be picked up by "#initialize"
    await this.storage.put("storageLimit", Infinity);

    // note that for a new room, "initialize" will fetch data from "this.storage" into object
    await this.#initialize(path[0])
      .catch(err => { return returnError(request, `Error initializing room [L1212]: ${err}`, 500) });
    if (DEBUG) console.log("CREATED channel:", this.#describe());
    return null; // null means no errors
  }

  async #getChannelKeys(request: Request) {
    const data: any = {};
    if (this.#channelKeyStrings) {
      data.ownerKey = this.#channelKeyStrings.ownerKey;
      if (this.#channelKeyStrings.guestKey)
        data.guestKey = this.#channelKeyStrings.guestKey;
      data.encryptionKey = this.#channelKeyStrings!.encryptionKey;
      data.signKey = this.#channelKeyStrings.signKey;
      return returnResult(request, JSON.stringify(data), 200);
    } else {
      return returnError(request, "Channel keys not initialized", 500);
    }
  }

  // used to create channels (from scratch), or upload from backup, or merge
  async #uploadData(request: Request) {
    if (DEBUG) console.log("==== uploadData() ====");
    if (!this.#channelKeys)
      return returnError(request, "UploadData but not initialized / created", 400);
    const _secret = this.env.SERVER_SECRET;
    const data = await request.arrayBuffer();
    const jsonString = new TextDecoder().decode(data);
    const jsonData = jsonParseWrapper(jsonString, 'L1416');
    if (DEBUG) {
      console.log("---- uploadData(): jsonData: ")
      console.log(jsonData)
    }
    const requestAuthorized = jsonData.hasOwnProperty("SERVER_SECRET") || jsonData["SERVER_SECRET"] === _secret;

    const { searchParams } = new URL(request.url);
    const targetChannel = searchParams.get('targetChannel');

    if ((requestAuthorized) && (jsonData.hasOwnProperty("size")) && (targetChannel === this.room_id)) {
      // we take our cue from size, see handleBuddRequest
      const size = Number(jsonData["size"]);
      _sb_assert(this.storageLimit !== undefined, "storageLimit undefined");
      const currentStorage = Number(await this.storage.get("storageLimit"));
      _sb_assert(currentStorage === this.storageLimit, "storage out of whatck");
      this.storageLimit += size;
      await this.storage.put("storageLimit", this.storageLimit);
      if (DEBUG) console.log(`uploadData(): increased budget by ${this.storageLimit} bytes`)
    }
    
    if ((this.room_owner === jsonData["roomOwner"]) || requestAuthorized) {
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
        } else if (key.slice(0, 64) === this.room_id) {
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
      return returnError(request, "Not authorized (neither owner keys nor admin credentials)", 401);
    }
  }

  async #authorizeRoom(request: Request) {
    const _secret = this.env.SERVER_SECRET;
    const jsonData: any = await request.json();
    const requestAuthorized = jsonData.hasOwnProperty("SERVER_SECRET") && jsonData["SERVER_SECRET"] === _secret;
    if (requestAuthorized) {
      // for (const key in jsonData) { } // TODO: any other keys to check?
      this.personalRoom = true;
      await this.storage.put("personalRoom", 'true');
      this.room_owner = jsonData["room_owner"];
      await this.storage.put("room_owner", jsonData["ownerKey"]);
      return returnResult(request, JSON.stringify({ success: true }), 200);
    } else {
      return returnError(request, "Cannot authorize room: server secret did not match", 401);
    }
  }
}
