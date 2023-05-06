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



import {
  arrayBufferToBase64, base64ToArrayBuffer,
  assemblePayload, extractPayload,
  jsonParseWrapper,
  ChannelKeys, SBChannelId,
  ChannelData,
  SBCrypto,
  ChannelAdminData
} from './snackabra.js';

const sbCrypto = new SBCrypto()

// this section has some type definitions that helps us with CF types
type EnvType = {
  // ChannelServerAPI
  channels: DurableObjectNamespace,

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

// for these two, see:
// https://developers.cloudflare.com/workers/runtime-apis/kv/#more-detail
type ListKey = {
  name: string;
  expiration?: number | undefined;
  metadata?: { [key: string]: string };
};

// same as KVNamespaceListResult but this is easier to deal with (rather than overloads)
type ListResult = {
  keys: ListKey[];
  list_complete: boolean;
  cursor?: string;
};

type messageDictionary = {
  [key: string]: string
}

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
      return new Response(null, { status: 101, webSocket: pair[0] });
    } else {
      return returnResult(request, err.stack, 500)
      //return new Response(err.stack, { status: 500 });
    }
  }
}

// // so many problems with JSON parsing, adding a wrapper to capture more info
// function jsonParseWrapper(str, loc) {
//   try {
//     return JSON.parse(str);
//   } catch (error) {
//     // sometimes it's an embedded string
//     try {
//       return JSON.parse(eval(str));
//     } catch {
//       // let's try one more thing
//       try {
//         return JSON.parse(str.slice(1, -1));
//       } catch {
//         // we'll throw the original error
//         throw new Error('JSON.parse() error at ' + loc + ' (tried eval and slice): ' + error.message + '\nString was: ' + str);
//       }
//     }
//   }
// }


function returnResult(_request: Request, contents: any, s: number, delay = 0) {
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

function returnError(_request: Request, errorString: string, status: number) {
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
 *     /api/room/<ID>/ownerKeyRotation    : [O]
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
          let reqData: any = await request.json();
          let roomList = reqData.rooms;
          let deviceIdentifier = reqData.identifier;
          if (DEBUG) console.log("Registering notifications", roomList, deviceIdentifier)
          for (let i = 0; i < roomList.length; i++) {
            if (DEBUG) { console.log("In loop"); console.log("ROOM ID", roomList[i]); }
            let id = env.channels.idFromName(roomList[i]);
            let roomObject = env.channels.get(id);
            let newUrl = new URL(request.url);
            newUrl.pathname = "/" + roomList[i] + "/registerDevice";
            newUrl.searchParams.append("id", deviceIdentifier)
            if (DEBUG) console.log("URL for room", newUrl)
            roomObject.fetch(newUrl, request);
          }
          return returnResult(request, JSON.stringify({ status: "Successfully received" }), 200);
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

// function postProduction(e, cb, delay, cbargs) {
//   e.waitUntil(new Promise(function (r) {
//     setTimeout(async function () {
//       await cb(cbargs);
//       r();
//     }, delay);
//   }));
// }


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

class ChannelConfig {
  storage: DurableObjectStorage
  #keys?: ChannelKeys
  ledgerKey: CryptoKey
  sessions = []

  #storageKeys: Dictionary<any> = {};

  // 'Ready Pattern'
  ready: Promise<ChannelConfig>
  channelConfigReady: Promise<ChannelConfig>
  #ChannelConfigReadyFlag: boolean = false // must be named <class>ReadyFlag

  // We keep track of the last-seen message's timestamp just so that we can assign monotonically
  // increasing timestamps even if multiple messages arrive simultaneously (see below).
  // lastTimestamp = 0;

  room_id: SBChannelId; // = '';

  constructor(room_id: SBChannelId, ledgerKey: CryptoKey, storage: DurableObjectStorage) {
    // TODO: this cannot handle exceptions ... 
    this.storage = storage;
    this.room_id = room_id;
    this.ledgerKey = ledgerKey;
    // this.room_owner = await storage.get('room_owner');
    // this.encryptionKey = await storage.get('encryptionKey');
    /*if (typeof this.room_owner === 'undefined' || typeof this.encryptionKey === 'undefined') {
      const keyFetch_json = await fetch("https://m063.dpn.workers.dev/api/v1/pubKeys?roomId=" + this.room_id)
      this.room_owner = JSON.stringify(keyFetch_json.ownerKey);
      this.encryptionKey = JSON.stringify(keyFetch_json.encryptionKey);
    }*/

    this.ready = new Promise<ChannelConfig>((resolve, reject) => {
      try {

        // default values
        this.#storageKeys = {
          'lastTimestamp': 0,
          'personalRoom': false,
          'room_capacity': 20,
          'visitors': [],
          'ownerUnread': 0,
          'locked': false,
          'join_requests': [],
          'accepted_requests': [],
          'storageLimit': 0,
          'lockedKeys': {},
          'deviceIds': [],
          'claimIat': 0,
          'motd': '',
          'motherChannel': '',
          'messagesCursor': null,
        };

        storage.get(Object.keys(this.#storageKeys)).then((values) => {
          Object.keys(this.#storageKeys).forEach((key) => {
            if (typeof values.get(key) !== 'undefined') this.#storageKeys[key] = values.get(key);
          });

        });


        let getKeys = ['ownerKey', 'guestKey', 'signKey', 'encryptionKey;']

        // // TODO: these are promises and need resolution
        // this.room_owner = this.getKey('ownerKey');
        // this.verified_guest = this.getKey('guestKey');
        // this.signKey = this.getKey('signKey');
        // this.encryptionKey = this.getKey('encryptionKey');

        /*
        for (let i = 0; i < this.accepted_requests.length; i++) {
          this.lockedKeys[this.accepted_requests[i]] = await storage.get(this.accepted_requests[i]);
        }
        */

        // TODO: remember when all is done we flip the flag
        this.#ChannelConfigReadyFlag = true
      } catch (e) {
        let errMsg = `failed to create ChannelConfig(): ${WrapError(e)}`
        console.error(errMsg)
        // _sb_exception("new Identity()", `failed to create Identity(): ${e}`) // do reject instead
        reject(errMsg)
      }
    })
    this.channelConfigReady = this.ready
  }

  /** @type {boolean}       */        get readyFlag() { return this.#ChannelConfigReadyFlag; }
  /** @type {number}        */ @Ready get room_capacity() { return this.#storageKeys['room_capacity'] as number }
  /**                       */        set room_capacity(newCapacity: number) { this.#storageKeys['room_capacity'] = newCapacity }

}


type ApiCallMap = {
  [key: string]: ((arg0: Request) => Promise<Response>) | undefined;
};


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

  sessions: Array<any> = [];

  config?: ChannelConfig;

  ownerCalls: ApiCallMap;
  visitorCalls: ApiCallMap;
  adminCalls: ApiCallMap;

  // We keep track of the last-seen message's timestamp just so that we can assign monotonically
  // increasing timestamps even if multiple messages arrive simultaneously (see below).

  room_id: SBChannelId = '';
  storageLimit: number = 0;

  // lastTimestamp: number = 0;
  // room_capacity: number = 20;
  // visitors: Array<string> = [];
  // ownerUnread: number = 0;
  // locked: boolean = false;
  // lockedKeys: any = {};
  // join_requests: Array<string> = [];
  // accepted_requests: Array<string> = [];
  // motd: string = '';
  // ledgerKey: CryptoKey | null = null;
  // deviceIds: Array<string> = [];
  // claimIat: number = 0;
  // notificationToken: any = {};
  // personalRoom: boolean = false;

  initializePromise: Promise<boolean> | null = null;

  // room_owner: JsonWebKey | null = null;
  // verified_guest: string | null = null;
  // encryptionKey: string | null = null;
  // signKey: string | null = null;

  channelKeys?: ChannelKeys;


  // 2023.05.xx: new fields
  motherChannel: string = '';
  messagesCursor: string | null = null;


  constructor(state: DurableObjectState, env: EnvType) {
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
      "/ownerKeyRotation": this.ownerKeyRotation.bind(this),
      "/ownerUnread": this.getOwnerUnreadMessages.bind(this),
      "/updateRoomCapacity": this.handleRoomCapacityChange.bind(this),
    }
    this.visitorCalls = {
      "/downloadData": this.downloadAllData.bind(this),
      "/oldMessages": this.handleOldMessages.bind(this),
      "/postPubKey": this.postPubKey.bind(this),
      "/registerDevice": this.registerDevice.bind(this),
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
            this.config = new ChannelConfig(room_id, ledgerKey, storage);
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

  // /**
  //  * initialize() is called either up on creation, or reload
  //  */
  // async initialize(room_id: SBChannelId) {
  //   const storage = this.storage;
  //   this.lastTimestamp = Number(await storage.get('lastTimestamp')) || 0;
  //   this.room_id = room_id;
  //   this.personalRoom = await storage.get('personalRoom') ? true : false;

  //   // this.room_owner = jsonParseWrapper(await this.getKey('ownerKey'), 'L301');
  //   // this.verified_guest = await this.getKey('guestKey');
  //   // this.signKey = await this.getKey('signKey');
  //   // this.encryptionKey = await this.getKey('encryptionKey');

  //   // SSO bootstrap code
  //   // if (typeof this.room_owner === 'undefined' || typeof this.encryptionKey === 'undefined') {
  //   //   const keyFetch_json: any = await fetch("https://m063.dpn.workers.dev/api/v1/pubKeys?roomId=" + this.room_id)
  //   //   this.room_owner = JSON.stringify(keyFetch_json.ownerKey);
  //   //   this.encryptionKey = JSON.stringify(keyFetch_json.encryptionKey);
  //   // }

  //   let ledgerKeyString = await this.getKey('ledgerKey');
  //   if (ledgerKeyString != null && ledgerKeyString !== "") {
  //     this.ledgerKey = await crypto.subtle.importKey(
  //       "jwk",
  //       jsonParseWrapper(ledgerKeyString, 'L217'),
  //       { name: "RSA-OAEP", hash: 'SHA-256' },
  //       true, ["encrypt"]) || null;
  //   }
  //   this.room_capacity = Number(await storage.get('room_capacity')) || 20;
  //   this.visitors = jsonParseWrapper(await storage.get('visitors') || JSON.stringify([]), 'L220');
  //   this.ownerUnread = Number(await storage.get('ownerUnread')) || 0;
  //   this.locked = await storage.get('locked') ? true : false;
  //   this.join_requests = jsonParseWrapper(await storage.get('join_requests') || JSON.stringify([]), 'L223');
  //   this.accepted_requests = jsonParseWrapper(await storage.get('accepted_requests') || JSON.stringify([]), 'L224');
  //   // this.storageLimit = await storage.get('storageLimit') || 4 * 1024 * 1024 * 1024; // PSM update 4 GiB default
  //   // 2023.05.03: new pattern, storageLimit should ALWAYS be present
  //   this.storageLimit = Number(await storage.get('storageLimit'))
  //   if (!this.storageLimit) {
  //     const ledgerData = await this.env.LEDGER_NAMESPACE.get(room_id);
  //     if (ledgerData) {
  //       const { size, mother } = jsonParseWrapper(ledgerData, 'L311');
  //       this.storageLimit = size
  //       this.motherChannel = mother
  //       if (DEBUG) console.log(`found storageLimit in ledger: ${this.storageLimit}`)
  //     } else {
  //       if (DEBUG) console.log("storageLimit is undefined in initialize, setting to default")
  //       this.storageLimit = NEW_CHANNEL_BUDGET;
  //     }
  //     await storage.put('storageLimit', this.storageLimit.toString());
  //   }
  //   this.lockedKeys = await (storage.get('lockedKeys')) || {};
  //   this.deviceIds = jsonParseWrapper(await (storage.get('deviceIds')) || JSON.stringify([]), 'L227');
  //   this.claimIat = Number(await storage.get('claimIat')) || 0;
  //   this.notificationToken = await storage.get('notificationToken') || "";
  //   // for (let i = 0; i < this.accepted_requests.length; i++)
  //   //   this.lockedKeys[this.accepted_requests[i]] = await storage.get(this.accepted_requests[i]);
  //   this.motd = await storage.get('motd') || '';

  //   // 2023.05.03 some new items
  //   if (!this.motherChannel) {
  //     this.motherChannel = await storage.get('motherChannel') || 'BOOTSTRAP';
  //   }
  //   await storage.put('motherChannel', this.motherChannel);
  //   if (DEBUG) console.log(`motherChannel: ${this.motherChannel}`)


  // }

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
      this.verified_guest = await this.getKey('guestKey');
    }
    return await handleErrors(request, async () => {
      let url = new URL(request.url);
      /* if (this.room_owner === '') {
        this.room_id = url.pathname.split('/')[1];
        const keyFetch_json = await fetch("https://m063.dpn.workers.dev/api/v1/pubKeys?roomId=" + this.room_id)
        this.room_owner = JSON.stringify(keyFetch_json.ownerKey);
        this.encryptionKey = JSON.stringify(keyFetch_json.encryptionKey);
      }*/
      this.room_id = url.pathname.split('/')[1];
      // url.pathname = "/" + url.pathname.split('/').slice(2).join("/");
      let url_pathname = "/" + url.pathname.split('/').slice(2).join("/")
      let new_url = new URL(url.origin + url_pathname)
      if (DEBUG2) {
        console.log("fetch() top new_url: ", new_url)
        console.log("fetch() top new_url.pathname: ", new_url.pathname)
      }
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
          if ((await this.verifyCookie(request) || await this.verifyAuthSign(request))) {
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
  async getRecentMessages(howMany: number, paginate = false): Promise<messageDictionary> {
    let storage: ListResult;
    if ((paginate) && (this.messagesCursor))
      storage = await this.storage.list({ limit: howMany, prefix: this.room_id, cursor: this.messagesCursor });
    else
      storage = await this.storage.list({ limit: howMany, prefix: this.room_id });
    if (storage.cursor)
      this.messagesCursor = storage.cursor;
    else
      this.messagesCursor = null;
    let keys: string[] = storage.keys.map((key) => key.name);
    keys.reverse();
    let messageList: { [key: string]: string } = {};
    let messageListPromises: { [key: string]: Promise<string | null> } = {};
    keys.map(async key =>
      messageListPromises[key] = this.storage.get(key, { type: 'text', cacheTtl: 86400 }));
    await Promise.all(Object.values(messageListPromises).map(async (promise, i) => {
      messageList[keys[i]] = jsonParseWrapper(await promise, 'L371');
    }));
    if (DEBUG) {
      console.log("getRecentMessages() messageList:")
      console.log(messageList)
    }
    return messageList;
  }

  async handleSession(webSocket: WebSocket, _ip: string | null) {
    webSocket.accept();
    // Create our session and add it to the sessions list.
    let session = {
      webSocket: webSocket,
      blockedMessages: {} as messageDictionary,
      quit: false
    };
    // We don't send any messages to the client until it has sent us 
    // the initial user info (message which would be the client's pubKey)
    session.blockedMessages = await this.getRecentMessages(100, false)
    this.sessions.push(session);

    var receivedUserInfo = false;

    webSocket.addEventListener("message", async msg => {
      try {
        if (session.quit) {
          webSocket.close(1011, "WebSocket broken.");
          console.log("webSocket broken")
          return;
        }

        if (!receivedUserInfo) {
          // The first message the client sends is the user info message with their pubKey. Save it
          // into their session object and in the visitor list.
          // webSocket.send(JSON.stringify({error: JSON.stringify(msg)}));
          const data = jsonParseWrapper(msg.data.toString(), 'L396');
          if (this.room_owner === null) {
            webSocket.close(4000, "This room does not have an owner, or the owner has not enabled it. You cannot leave messages here.");
            console.log("no owner - closing")
          }
          let keys: ChannelKeys = {
            ownerKey: this.room_owner,
            guestKey: this.verified_guest,
            encryptionKey: this.encryptionKey,
            signKey: this.signKey
          };
          if (data.pem) {
            keys = await this.convertToPem({
              ownerKey: this.room_owner,
              guestKey: this.verified_guest,
              encryptionKey: this.encryptionKey,
              signKey: this.signKey
            });
          }
          if (!data.name) {
            webSocket.close(1000, 'The first message should contain the pubKey')
            console.log("no pubKey")
            return;
          }
          // const isPreviousVisitor = this.visitors.indexOf(data.name) > -1;
          // const isAccepted = this.accepted_requests.indexOf(data.name) > -1;
          let _name;
          try {
            _name = jsonParseWrapper(data.name, 'L412');
          } catch (err) {
            webSocket.close(1000, 'The first message should contain the pubKey in json stringified format')
            console.log("improper pubKey")
            return;
          }
          const isPreviousVisitor = this.checkJsonExistence(_name, this.visitors);
          const isAccepted = this.checkJsonExistence(_name, this.accepted_requests);
          if (!isPreviousVisitor && this.visitors.length >= this.room_capacity) {
            webSocket.close(4000, 'The room is not accepting any more visitors.');
            console.log("no more visitors")
            return;
          }
          if (!isPreviousVisitor) {
            this.visitors.push(data.name);
            this.storage.put('visitors', JSON.stringify(this.visitors))
          }
          if (this.locked) {
            if (!isAccepted && !isPreviousVisitor) {
              this.join_requests.push(data.name);
              this.storage.put('join_requests', JSON.stringify(this.join_requests));
            } else {
              // const encrypted_key = this.lockedKeys[data.name];
              const encrypted_key = this.lockedKeys[this.getLockedKey(_name)];
              keys['locked_key'] = encrypted_key;
            }

          }
          session.name = data.name;
          webSocket.send(JSON.stringify({ ready: true, keys: keys, motd: this.motd, roomLocked: this.locked }));
          // session.room_id = "" + data.room_id;
          // Deliver all the messages we queued up since the user connected.


          // Note that we've now received the user info message.
          receivedUserInfo = true;

          return;
        } else if (jsonParseWrapper(msg.data, 'L449').ready) {
          if (!session.blockedMessages) {
            return;
          }
          if (this.env.DOCKER_WS) {
            session.blockedMessages.forEach(queued => {
              webSocket.send(queued);
            });
            delete session.blockedMessages;
            return;
          }
          webSocket.send(session.blockedMessages)
          delete session.blockedMessages;
          return;

        }
        this.ownerUnread += 1;
        let ts = Math.max(Date.now(), this.lastTimestamp + 1);
        this.lastTimestamp = ts;
        this.storage.put('lastTimestamp', this.lastTimestamp)
        ts = ts.toString(2);
        while (ts.length < 42) ts = "0" + ts;
        const key = this.room_id + ts;

        let _x = {}
        _x[key] = jsonParseWrapper(msg.data, 'L466');
        await this.broadcast(JSON.stringify(_x))
        await this.storage.put(key, msg.data);


        // await this.sendNotifications(true);

        // webSocket.send(JSON.stringify({ error: err.stack }));
        await this.env.MESSAGES_NAMESPACE.put(key, msg.data);
      } catch (error) {
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

    // On "close" and "error" events, remove the WebSocket from the sessions list and broadcast
    // a quit message.
    let closeOrErrorHandler = evt => {
      session.quit = true;
      this.sessions = this.sessions.filter(member => member !== session);
    };
    webSocket.addEventListener("close", closeOrErrorHandler);
    webSocket.addEventListener("error", closeOrErrorHandler);
  }

  // broadcast() broadcasts a message to all clients.
  async broadcast(message: string) {
    if (typeof message !== "string") {
      message = JSON.stringify(message);
    }
    if (DEBUG) console.log("calling sendWebNotifications()", message);
    await this.sendWebNotifications(message);
    // Iterate over all the sessions sending them messages.
    let quitters = [];
    this.sessions = this.sessions.filter(session => {
      if (session.name) {
        try {
          session.webSocket.send(message);
          if (session.name === this.room_owner) {
            this.ownerUnread -= 1;
          }
          return true;
        } catch (err) {
          session.quit = true;
          quitters.push(session);
          return false;
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

  // async handleOldMessages(request: Request) {
  //   try {
  //     const { searchParams } = new URL(request.url);
  //     const currentMessagesLength = searchParams.get('currentMessagesLength');
  //     let storage = await this.storage.list({
  //       reverse: true,
  //       limit: 100 + currentMessagesLength,
  //       prefix: this.room_id
  //     });
  //     let keys = [...storage.keys()];
  //     keys = keys.slice(currentMessagesLength);
  //     keys.reverse();
  //     let backlog = {}
  //     keys.forEach(key => {
  //       try {
  //         backlog[key] = jsonParseWrapper(storage.get(key), 'L531');
  //       } catch (error) {
  //         console.log(error)
  //       }
  //     })

  //     return returnResult(request, JSON.stringify(backlog), 200);
  //   } catch (error) {
  //     console.log("Error fetching older messages: ", error)
  //     return returnResult(request, JSON.stringify({ error: "Could not fetch older messages" }), 500)
  //   }
  // }

  async handleOldMessages(request: Request) {
    try {
      const { searchParams } = new URL(request.url);
      const currentMessagesLength = Number(searchParams.get('currentMessagesLength')) || 100;
      const paginate = searchParams.get('paginate') === 'true' ? true : false;
      return returnResult(request, JSON.stringify(this.getRecentMessages(currentMessagesLength, paginate)), 200);
    } catch (error: any) {
      console.log("Error fetching older messages: ", error)
      return returnResult(request, JSON.stringify({ error: "Could not fetch older messages: " + error.message }), 500)
    }
  }

  async getKey(type: string): Promise<string | null> {
    if (this.personalRoom) {
      // keys managed by owner
      if (type === 'ledgerKey') return this.env.LEDGER_KEY;
      const ret = await this.storage.get(type);
      if (ret) return(ret as string)
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

  async postPubKey(request: Request) {
    // TODO: huh? this allows any key?
    try {
      const { searchParams } = new URL(request.url);
      // const json = await request.json();
      const str = await request.text();
      if (str) {
        const json = await jsonParseWrapper(str, 'L611');
        const keyType = searchParams.get('type');
        if (keyType != null) {
          this.storage.put(keyType, JSON.stringify(json));
          this.initialize();
        }
        return returnResult(request, JSON.stringify({ success: true }), 200);
      } else {
        return returnResult(request, JSON.stringify({ success: false, error: '[postPubKey()] Received empty request body (no key)' }), 500);
      }
    } catch (error: any) {
      console.log("ERROR posting pubKey", error);
      return returnResult(request, JSON.stringify({
        success: false,
        error: '[postPubKey()] ' + error.message + '\n' + error.stack
      }), 200)
    }
  }

  async handleRoomCapacityChange(request: Request) {
    const { searchParams } = new URL(request.url);
    const newLimit = searchParams.get('capacity');
    this.room_capacity = newLimit;
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
    let data;
    data = await request.json();
    const ind = this.join_requests.indexOf(data.pubKey);
    if (ind > -1) {
      this.accepted_requests = [...this.accepted_requests, ...this.join_requests.splice(ind, 1)];
      this.lockedKeys[data.pubKey] = data.lockedKey;
      this.storage.put('accepted_requests', JSON.stringify(this.accepted_requests));
      this.storage.put('lockedKeys', this.lockedKeys);
      this.storage.put('join_requests', JSON.stringify(this.join_requests))
      return returnResult(request, JSON.stringify({}), 200);
    }
    return returnResult(request, JSON.stringify({ error: "Could not accept visitor" }), 500)
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
    this.motd = data.motd;
    this.storage.put('motd', this.motd);
    return returnResult(request, JSON.stringify({ motd: this.motd }), 200);
  }

  async ownerKeyRotation(request: Request) {
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
    // this.broadcast(JSON.stringify({ control: true, ownerKeyChanged: true, ownerKey: _updatedKey }));
    this.room_owner = _updatedKey;

    // Now pushing all accepted requests back to join requests
    this.join_requests = [...this.join_requests, ...this.accepted_requests];
    this.accepted_requests = [];
    this.lockedKeys = [];
    this.storage.put('join_requests', JSON.stringify(this.join_requests))
    this.storage.put('lockedKeys', JSON.stringify(this.lockedKeys))
    this.storage.put('accepted_requests', JSON.stringify(this.accepted_requests));
    return returnResult(request, JSON.stringify({ success: true }), 200);
  }

  async checkRotation(_timeout) {
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
    const encrypted_token_id = arrayBufferToBase64(await crypto.subtle.encrypt({ name: "RSA-OAEP" }, this.ledgerKey, token_buffer));
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
        return returnResult(request, JSON.stringify({ error: `Not enough storage request for a new channel (minimum is ${this.NEW_CHANNEL_MINIMUM_BUDGET} bytes)` }), 500);
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

  async verifyAuthSign(request: Request): Promise<boolean> {
    if (!request.headers.has('authorization')) {
      return false;
    }
    let authHeader = request.headers.get('authorization');
    if (!authHeader) return false
    let auth_parts = authHeader.split('.');
    if (new Date().getTime() - parseInt(auth_parts[0]) > 60000) {
      return false;
    }
    let sign = auth_parts[1];
    let ownerKey = await crypto.subtle.importKey("jwk", this.room_owner, {
      name: "ECDH",
      namedCurve: "P-384"
    }, false, ["deriveKey"]);
    let roomSignKey = await crypto.subtle.importKey("jwk", this.signKey, {
      name: "ECDH",
      namedCurve: "P-384"
    }, false, ["deriveKey"]);
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

  async verifySign(secretKey, sign, contents) {
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

  async sign(secretKey, contents) {
    const encoder = new TextEncoder();
    const encoded = encoder.encode(contents);
    let sign;
    try {
      sign = await crypto.subtle.sign(
        'HMAC',
        secretKey,
        encoded
      );
      return encodeURIComponent(arrayBufferToBase64(sign));
    } catch (error) {
      // console.log(error);
      return { error: "Failed to sign content" };
    }
  }

  async jwtSign(payload, secret, options) {
    if (DEBUG) console.log("Trying to create sign: ", payload, typeof payload, secret, typeof secret)
    if (payload === null || typeof payload !== 'object')
      throw new Error('payload must be an object')
    if (typeof secret !== 'string')
      throw new Error('secret must be a string')
    const importAlgorithm = { name: 'ECDSA', namedCurve: 'P-256', hash: { name: 'SHA-256' } }

    if (DEBUG) console.log("PAST THROWS")
    payload.iat = Math.floor(Date.now() / 1000)
    this.claimIat = payload.iat;
    this.storage.put("claimIat", this.claimIat)
    const payloadAsJSON = JSON.stringify(payload)
    const partialToken = `${this.jwtStringify(this._utf8ToUint8Array(JSON.stringify({
      alg: options.algorithm,
      kid: options.keyid
    })))}.${this.jwtStringify(this._utf8ToUint8Array(payloadAsJSON))}`
    let keyFormat = 'raw'
    let keyData
    if (secret.startsWith('-----BEGIN')) {
      keyFormat = 'pkcs8'
      keyData = this.jwt_str2ab(atob(secret.replace(/-----BEGIN.*?-----/g, '').replace(/-----END.*?-----/g, '').replace(/\s/g, '')))
    } else
      keyData = this._utf8ToUint8Array(secret)
    const key = await crypto.subtle.importKey(keyFormat, keyData, importAlgorithm, false, ['sign'])
    if (DEBUG) console.log("GOT KEY: ", key);
    const signature = await crypto.subtle.sign(importAlgorithm, key, this._utf8ToUint8Array(partialToken))
    return `${partialToken}.${this.jwtStringify(new Uint8Array(signature))}`
  }

  _utf8ToUint8Array(str) {
    return this.jwtParse(btoa(unescape(encodeURIComponent(str))))
  }

  jwtParse(s) {
    return new Uint8Array(Array.prototype.map.call(atob(s.replace(/-/g, '+').replace(/_/g, '/').replace(/\s/g, '')), c => c.charCodeAt(0)))
  }

  jwtStringify(a) {
    return btoa(String.fromCharCode.apply(0, a)).replace(/=/g, '').replace(/\+/g, '-').replace(/\//g, '_')
  }

  jwt_str2ab(str) {
    const buf = new ArrayBuffer(str.length);
    const bufView = new Uint8Array(buf);
    for (let i = 0, strLen = str.length; i < strLen; i++) {
      bufView[i] = str.charCodeAt(i);
    }
    return buf;
  }

  addNewlines(str) {
    var result = '';
    while (str.length > 64) {
      result += str.substring(0, 64) + '\n';
      str = str.substring(64);
    }
    result += str
    return result;
  }

  ab2str(buf) {
    return String.fromCharCode.apply(null, new Uint8Array(buf));
  }

  /*
  Export the given key and write it into the "exported-key" space.
  */
  async exportPrivateCryptoKey(key) {
    const exported = await crypto.subtle.exportKey(
      "pkcs8",
      key
    );
    const exportedAsString = this.ab2str(exported);
    const exportedAsBase64 = this.addNewlines(btoa(exportedAsString))
    const pemExported = `-----BEGIN PRIVATE KEY-----\n${exportedAsBase64}\n-----END PRIVATE KEY-----`;

    return pemExported
  }

  async exportPublicCryptoKey(key) {
    const exported = await crypto.subtle.exportKey(
      "spki",
      key
    );
    const exportedAsString = this.ab2str(exported);
    const exportedAsBase64 = this.addNewlines(btoa(exportedAsString));
    const pemExported = `-----BEGIN PUBLIC KEY-----\n${exportedAsBase64}\n-----END PUBLIC KEY-----`;

    return pemExported;
  }

  // async convertToPem(keys) {
  //   let _keys = {};
  //   for (let key in keys) {
  //     try {
  //       if (keys[key] === null || keys[key] === "") {
  //         continue;
  //       }
  //       const keyType = key.split('_').slice(-1)[0];
  //       let val = typeof keys[key] === 'object' ? keys[key] : jsonParseWrapper(keys[key], 'L956');
  //       if (keyType === 'encryptionKey') {
  //         const cryptoKey = await crypto.subtle.importKey("jwk", val, {
  //           name: "AES-GCM",
  //           length: 256
  //         }, true, ['encrypt', 'decrypt']);
  //         const exported = await crypto.subtle.exportKey("raw", cryptoKey);
  //         _keys[key] = btoa(this.ab2str(exported));
  //       } else if (keyType === 'signKey') {
  //         const cryptoKey = await crypto.subtle.importKey("jwk", val, {
  //           name: "ECDH",
  //           namedCurve: "P-384"
  //         }, true, ['deriveKey']);
  //         const _pemKey = await this.exportPrivateCryptoKey(cryptoKey);
  //         _keys[key] = _pemKey;
  //       } else {
  //         const cryptoKey = await crypto.subtle.importKey("jwk", val, { name: "ECDH", namedCurve: "P-384" }, true, []);
  //         const _pemKey = await this.exportPublicCryptoKey(cryptoKey);
  //         _keys[key] = _pemKey;
  //       }
  //     } catch {
  //       _keys[key] = "ERROR"
  //     }
  //   }
  //   return _keys;
  // }

  // base64ToArrayBuffer(base64) {
  //   var binary_string = atob(base64);
  //   var len = binary_string.length;
  //   var bytes = new Uint8Array(len);
  //   for (var i = 0; i < len; i++) {
  //     bytes[i] = binary_string.charCodeAt(i);
  //   }
  //   return bytes.buffer;
  // }

  // arrayBufferToBase64(buffer) {
  //   // try {  // better to just throw the error

  //   let binary = '';
  //   const bytes = new Uint8Array(buffer);
  //   const len = bytes.byteLength;
  //   for (let i = 0; i < len; i++) {
  //     binary += String.fromCharCode(bytes[i]);
  //   }
  //   return btoa(binary);

  //   // }
  //   // catch (e) {
  //   //   console.log(e);
  //   //   return { error: e };
  //   // }
  // }

  checkJsonExistence(val, arr) {
    for (let i = 0; i < arr.length; i++) {
      try {
        if (this.areJsonKeysSame(val, jsonParseWrapper(arr[i], 'L1008')))
          return true;
      } catch (err) {
        continue;
      }
    }
    return false;
  }

  getLockedKey(val) {
    for (let key of Object.keys(this.lockedKeys)) {
      if (this.areJsonKeysSame(val, jsonParseWrapper(key, 'L1019')))
        return this.lockedKeys[key];
    }
    return { error: "Could not find key" };
  }

  areJsonKeysSame(key1, key2) {
    return (key1["x"] == key2["x"]) && (key1["y"] == key2["y"]);
  }

  registerDevice(request: Request) {
    if (DEBUG) console.log("Registering device")
    const { searchParams } = new URL(request.url);
    this.deviceIds = [...new Set(this.deviceIds)];
    let deviceId = searchParams.get("id");
    if (!this.deviceIds.includes(deviceId)) {
      this.deviceIds.push(deviceId);
    }
    if (DEBUG) console.log(this.deviceIds);
    this.storage.put("deviceIds", JSON.stringify(this.deviceIds));
    return returnResult(request, JSON.stringify({ success: true }), 200);
  }

  async sendNotifications(dev = true) {
    if (DEBUG) console.log("Trying to send notifications", this.claimIat)
    let jwtToken = this.notificationToken;
    if ((this.claimIat - (Math.round(new Date().getTime() / 1000))) > 3000 || jwtToken === "" && this.env.ISS && this.env.APS_KEY) {
      if (DEBUG) console.log("Refreshing token")
      const claim = { iss: this.env.ISS };
      const pemKey = this.env.APS_KEY;
      // console.log("IN IF CLAUSE", claim, pemKey);
      try {
        jwtToken = await this.jwtSign(claim, pemKey, { algorithm: "ES256", keyid: this.env.KID });
        this.storage.put("notificationToken", jwtToken)
      } catch (err) {
        console.log("Error signing with jwt: ", err.message);
      }
      this.notificationToken = jwtToken;
    }
    let notificationJSON = {
      aps: {
        alert: {
          title: this.room_id,
          subtitle: "You have a new message!"
        },
        sound: "default",
      }
    };
    if (this.env.ISS && this.env.APS_KEY) {
      notificationJSON.aps["mutable-content"] = true;
      notificationJSON.aps["interruption-level"] = "active";
      let notification = JSON.stringify(notificationJSON)
      if (DEBUG) console.log("TOKEN", jwtToken, this.deviceIds[0], notification)
      let base = dev ? "https://api.sandbox.push.apple.com/3/device/" : "https://api.push.apple.com/3/device/"
      // console.log("DEVICE LENGTH: ", this.deviceIds.length)
      for (let i = 0; i < this.deviceIds.length; i++) {
        if (DEBUG) console.log("Pinging: ", this.deviceIds[i]);
        const device_token = this.deviceIds[i];
        let url = base + device_token;
        let request = {
          method: "POST",
          headers: {
            "authorization": "bearer " + jwtToken,
            "apns-topic": "app.snackabra",
            "apns-push-type": "alert"
          },
          body: notification
        }
        let req = await fetch(url, request);
        if (DEBUG) console.log("Request is: ", url, JSON.stringify(request))
        if (DEBUG) console.log('APPLE RESPONSE', req);
      }
    } else {
      if (DEBUG) console.log('Set ISS and APS_KEY env vars to enable apple notifications')
    }

  }

  async sendWebNotifications(message) {
    if (DEBUG) console.log("Sending web notification", message)
    message = JSON.parse(message)
    if (message?.type === 'ack') return
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
    let storage = await this.storage.list();
    let data = {
      roomId: this.room_id,
      ownerKey: this.room_owner,
      encryptionKey: this.encryptionKey,
      guestKey: this.verified_guest,
      signKey: this.signKey
    };
    // TODO: dangerous, only download approved parts
    storage.forEach((value, key, map) => {
      data[key] = value;
    });
    if (!this.verifyCookie(request)) {
      delete data.room_capacity;
      delete data.visitors;
      delete data.ownerUnread;
      delete data.join_requests;
      delete data.accepted_requests;
      delete data.storageLimit;
      delete data.lockedKeys;
      delete data.deviceIds;
      delete data.claimIat;
      delete data.notificationToken;
    }
    let dataBlob = new TextEncoder().encode(JSON.stringify(data));
    // return new Response(dataBlob, { status: 200, headers: corsHeaders });
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
