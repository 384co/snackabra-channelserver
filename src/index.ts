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
  SBCrypto
} from './snackabra.js';

const sbCrypto = new SBCrypto()


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


async function handleErrors(request: Request, func: () => Promise<Response | undefined>) {
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


function returnResult(_request: Request, contents: any, s: number) {
  const corsHeaders = {
    "Access-Control-Allow-Methods": "POST, OPTIONS, GET",
    "Access-Control-Allow-Headers": "Content-Type, authorization",
    "Access-Control-Allow-Credentials": "true",
    'Content-Type': 'application/json;',
    "Access-Control-Allow-Origin": "*" /* request.headers.get("Origin") */,
  }
  if (DEBUG) console.log("returnResult() contents:", contents, "status:", s)
  return new Response(contents, { status: s, headers: corsHeaders });
}


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
  switch (path[0]) {
    case "room": {
      // request is for '/api/room/...', we route (relay) it to the matching durable object
      // TODO: many requests can probably be handled here, without routing to the durable object
      return callDurableObject(path[1], path.slice(2), request, env);
    }
    case "notifications": {
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
    case "getLastMessageTimes": {
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


/**
 *
 * ChannelServer Durable Object Class
 * 
 *     
 * Note: historically channels were referred to as 'rooms'.
 */
export class ChannelServer {
  storage: KVNamespace;
  env: EnvType;

  sessions: Array<any> = [];
  // We keep track of the last-seen message's timestamp just so that we can assign monotonically
  // increasing timestamps even if multiple messages arrive simultaneously (see below).
  lastTimestamp: number = 0;
  room_id: SBChannelId = '';
  room_owner: JsonWebKey | null = null;
  room_capacity: number = 20;
  visitors: Array<string> = [];
  ownerUnread: number = 0;
  locked: boolean = false;
  encryptionKey: string | null = null;
  lockedKeys: any = {};
  join_requests: Array<string> = [];
  accepted_requests: Array<string> = [];
  motd: string = '';
  verified_guest: string | null = null;
  signKey: string | null = null;
  storageLimit: number = 0;
  ledgerKey: CryptoKey | null = null;
  deviceIds: Array<string> = [];
  claimIat: number = 0;
  notificationToken: any = {};
  personalRoom: boolean = false;
  initializePromise: Promise<void> | null = null;
  // 2023.04.03: new fields
  motherChannel: string = '';


  constructor(state: any, env: EnvType) {
    this.storage = state.storage;
    this.env = env;
  }

  /**
   * initialize() is called either up on creation, or reload
   */
  async initialize(room_id: SBChannelId) {
    const storage = this.storage;
    this.lastTimestamp = Number(await storage.get('lastTimestamp')) || 0;
    this.room_id = room_id;
    this.personalRoom = await storage.get('personalRoom') ? true : false;
    this.room_owner = jsonParseWrapper(await this.getKey('ownerKey'), 'L301');
    this.verified_guest = await this.getKey('guestKey');
    this.signKey = await this.getKey('signKey');
    this.encryptionKey = await this.getKey('encryptionKey');

    // SSO bootstrap code
    // if (typeof this.room_owner === 'undefined' || typeof this.encryptionKey === 'undefined') {
    //   const keyFetch_json: any = await fetch("https://m063.dpn.workers.dev/api/v1/pubKeys?roomId=" + this.room_id)
    //   this.room_owner = JSON.stringify(keyFetch_json.ownerKey);
    //   this.encryptionKey = JSON.stringify(keyFetch_json.encryptionKey);
    // }

    let ledgerKeyString = await this.getKey('ledgerKey');
    if (ledgerKeyString != null && ledgerKeyString !== "") {
      this.ledgerKey = await crypto.subtle.importKey(
        "jwk",
        jsonParseWrapper(ledgerKeyString, 'L217'),
        { name: "RSA-OAEP", hash: 'SHA-256' },
        true, ["encrypt"]) || null;
    }
    this.room_capacity = Number(await storage.get('room_capacity')) || 20;
    this.visitors = jsonParseWrapper(await storage.get('visitors') || JSON.stringify([]), 'L220');
    this.ownerUnread = Number(await storage.get('ownerUnread')) || 0;
    this.locked = await storage.get('locked') ? true : false;
    this.join_requests = jsonParseWrapper(await storage.get('join_requests') || JSON.stringify([]), 'L223');
    this.accepted_requests = jsonParseWrapper(await storage.get('accepted_requests') || JSON.stringify([]), 'L224');
    // this.storageLimit = await storage.get('storageLimit') || 4 * 1024 * 1024 * 1024; // PSM update 4 GiB default
    // 2023.05.03: new pattern, storageLimit should ALWAYS be present
    this.storageLimit = Number(await storage.get('storageLimit'))
    if (!this.storageLimit) {
      const ledgerData = await this.env.LEDGER_NAMESPACE.get(room_id);
      if (ledgerData) {
        const { size, mother } = jsonParseWrapper(ledgerData, 'L311');
        this.storageLimit = size
        this.motherChannel = mother
        if (DEBUG) console.log(`found storageLimit in ledger: ${this.storageLimit}`)
      } else {
        if (DEBUG) console.log("storageLimit is undefined in initialize, setting to default")
        this.storageLimit = NEW_CHANNEL_BUDGET;
      }
      await storage.put('storageLimit', this.storageLimit.toString());
    }
    this.lockedKeys = await (storage.get('lockedKeys')) || {};
    this.deviceIds = jsonParseWrapper(await (storage.get('deviceIds')) || JSON.stringify([]), 'L227');
    this.claimIat = Number(await storage.get('claimIat')) || 0;
    this.notificationToken = await storage.get('notificationToken') || "";
    // for (let i = 0; i < this.accepted_requests.length; i++)
    //   this.lockedKeys[this.accepted_requests[i]] = await storage.get(this.accepted_requests[i]);
    this.motd = await storage.get('motd') || '';

    // 2023.05.03 some new items
    if (!this.motherChannel) {
      this.motherChannel = await storage.get('motherChannel') || 'BOOTSTRAP';
    }
    await storage.put('motherChannel', this.motherChannel);
    if (DEBUG) console.log(`motherChannel: ${this.motherChannel}`)

    if (DEBUG) {
      console.log("Done creating room:")
      console.log("room_id: ", this.room_id)
      if (DEBUG2) console.log(this)
    }
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

      switch (new_url.pathname) {
        case "/websocket": {
          if (request.headers.get("Upgrade") != "websocket") {
            return new Response("expected websocket", { status: 400 });
          }
          // this.room_id = request.url;
          let ip = request.headers.get("CF-Connecting-IP");
          let pair = new WebSocketPair();
          await this.handleSession(pair[1], ip);
          return new Response(null, { status: 101, webSocket: pair[0] });
        }

        case "/oldMessages": {
          return await this.handleOldMessages(request);
        }
        case "/updateRoomCapacity": {
          return (await this.verifyCookie(request) || await await this.verifyAuthSign(request)) ? await this.handleRoomCapacityChange(request) : returnResult(request, JSON.stringify({ error: "Owner verification failed" }, 500));
          // return await this.handleRoomCapacityChange(request);
        }
        case "/getRoomCapacity": {
          return (await this.verifyCookie(request) || await this.verifyAuthSign(request)) ? await this.getRoomCapacity(request) : returnResult(request, JSON.stringify({ error: "Owner verification failed" }, 500));
          // return await this.getRoomCapacity(request);
        }
        case "/getPubKeys": {
          return (await this.verifyCookie(request) || await this.verifyAuthSign(request)) ? await this.getPubKeys(request) : returnResult(request, JSON.stringify({ error: "Owner verification failed" }, 500));
          // return await this.getPubKeys(request);
        }
        case "/getJoinRequests": {
          return (await this.verifyCookie(request) || await this.verifyAuthSign(request)) ? await this.getJoinRequests(request) : returnResult(request, JSON.stringify({ error: "Owner verification failed" }, 500));
          // return await this.getJoinRequests(request);
        }
        case "/lockRoom": {
          return (await this.verifyCookie(request) || await this.verifyAuthSign(request)) ? await this.lockRoom(request) : returnResult(request, JSON.stringify({ error: "Owner verification failed" }, 500));
          // return await this.lockRoom(request);
        }
        case "/acceptVisitor": {
          return (await this.verifyCookie(request) || await this.verifyAuthSign(request)) ? await this.acceptVisitor(request) : returnResult(request, JSON.stringify({ error: "Owner verification failed" }, 500));
          // return await this.acceptVisitor(request);
        }
        case "/roomLocked": {
          return await this.isRoomLocked(request);
          // return await this.isRoomLocked(request);
        }
        case "/ownerUnread": {
          return (await this.verifyCookie(request) || await this.verifyAuthSign(request)) ? await this.getOwnerUnreadMessages(request) : returnResult(request, JSON.stringify({ error: "Owner verification failed" }, 500));
        }
        case "/motd": {
          return await this.setMOTD(request);
        }
        case "/ownerKeyRotation": {
          return await this.ownerKeyRotation(request);
        }
        case "/storageRequest": {
          return await this.handleNewStorage(request);
        }
        case "/getAdminData": {
          return (await this.verifyCookie(request) || await this.verifyAuthSign(request)) ? await this.handleAdminDataRequest(request) : returnResult(request, JSON.stringify({ error: "Owner verification failed" }, 500));
        }
        case "/registerDevice": {
          return this.registerDevice(request);
        }
        case "/downloadData": {
          return await this.downloadAllData(request);
        }
        case "/uploadRoom": {
          return await this.uploadData(request);
        }
        case "/budd": {
          return (await this.verifyCookie(request) || await this.verifyAuthSign(request)) ? await this.handleBuddRequest(request) : returnResult(request, JSON.stringify({ error: "Owner verification failed" }, 500));
        }
        case "/getStorageLimit": {
          return (await this.verifyCookie(request) || await this.verifyAuthSign(request)) ? await this.getStorageLimit(request) : returnResult(request, JSON.stringify({ error: "Owner verification failed" }, 500));
        }
        case "/getMother": {
          // only owner (or hosting provider) can get mother
          return (await this.verifyCookie(request) || await this.verifyAuthSign(request)) ? await this.getMother(request) : returnResult(request, JSON.stringify({ error: "Owner verification failed" }, 500));
        }
        case "/authorizeRoom": {
          return await this.authorizeRoom(request);
        }
        case "/postPubKey": {
          return await this.postPubKey(request);
        }
        default:
          return returnResult(request, JSON.stringify({ error: "Not found " + new_url.pathname }), 404)
        //return new Response("Not found", { status: 404 });
      }
    });
  }

  async handleSession(webSocket, _ip) {
    //await this.initialize();
    webSocket.accept();
    // Create our session and add it to the sessions list.
    // We don't send any messages to the client until it has sent us the initial user info
    // message which would be the client's pubKey
    let session = { webSocket, blockedMessages: [] };
    this.sessions.push(session);

    let storage = await this.storage.list({ reverse: true, limit: 100, prefix: this.room_id });
    let keys = [...storage.keys()];
    keys.reverse();
    let backlog = {}
    keys.forEach(key => {
      try {
        backlog[key] = jsonParseWrapper(storage.get(key), 'L371');
      } catch (error) {
        webSocket.send(JSON.stringify({ error: '[handleSession()] ' + error.message + '\n' + error.stack }));
      }
    })
    //session.blockedMessages.push(JSON.stringify({...storage}))
    //let backlog = [...storage.values()];
    //backlog.reverse();
    //backlog.forEach(value => {
    //  session.blockedMessages.push(value);
    //});
    session.blockedMessages = [...session.blockedMessages, JSON.stringify(backlog)]
    // Set event handlers to receive messages.
    let receivedUserInfo = false;
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
          const data = jsonParseWrapper(msg.data, 'L396');
          if (this.room_owner === null || this.room_owner === "") {
            webSocket.close(4000, "This room does not have an owner, or the owner has not enabled it. You cannot leave messages here.");
            console.log("no owner - closing")
          }
          let keys = {
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
  async broadcast(message) {
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

  async handleOldMessages(request) {
    try {
      const { searchParams } = new URL(request.url);
      const currentMessagesLength = searchParams.get('currentMessagesLength');
      let storage = await this.storage.list({
        reverse: true,
        limit: 100 + currentMessagesLength,
        prefix: this.room_id
      });
      let keys = [...storage.keys()];
      keys = keys.slice(currentMessagesLength);
      keys.reverse();
      let backlog = {}
      keys.forEach(key => {
        try {
          backlog[key] = jsonParseWrapper(storage.get(key), 'L531');
        } catch (error) {
          console.log(error)
        }
      })

      return returnResult(request, JSON.stringify(backlog), 200);
    } catch (error) {
      console.log("Error fetching older messages: ", error)
      return returnResult(request, JSON.stringify({ error: "Could not fetch older messages" }), 500)
    }
  }

  async getKey(type: string): Promise<string | null> {
    if (this.personalRoom) {
      if (type === 'ledgerKey') {
        return this.env.LEDGER_KEY;
      }
      return await this.storage.get(type);
    }
    if (type === 'ownerKey') {
      let _keys_id = (await this.env.KEYS_NAMESPACE.list({ prefix: this.room_id + '_ownerKey' })).keys.map(key => key.name);
      if (_keys_id.length == 0) {
        return null;
      }
      let keys = _keys_id.map(async key => await this.env.KEYS_NAMESPACE.get(key));
      return await keys[keys.length - 1];
    } else if (type === 'ledgerKey') {
      return await this.env.KEYS_NAMESPACE.get(type);
    }
    return await this.env.KEYS_NAMESPACE.get(this.room_id + '_' + type);
  }

  async postPubKey(request: Request) {
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
        console.log("ERROR: Received blank body in postPubKey(request) (??)");
        return returnResult(request, JSON.stringify({
          success: false,
          error: '[postPubKey()] Received empty request body (??)\n'
        }), 200);
      }
    } catch (error) {
      console.log("ERROR posting pubKey", error);
      return returnResult(request, JSON.stringify({
        success: false,
        error: '[postPubKey()] ' + error.message + '\n' + error.stack
      }), 200)
    }
  }

  async handleRoomCapacityChange(request) {
    try {
      const { searchParams } = new URL(request.url);
      const newLimit = searchParams.get('capacity');
      this.room_capacity = newLimit;
      this.storage.put('room_capacity', this.room_capacity)
      return returnResult(request, JSON.stringify({ capacity: newLimit }), 200);
    } catch (error) {
      console.log("Could not change room capacity: ", error);
      return returnResult(request, JSON.stringify({ error: "Could not change room capacity" }), 500)
    }
  }

  async getRoomCapacity(request) {
    try {
      return returnResult(request, JSON.stringify({ capacity: this.room_capacity }), 200);
    } catch (error) {
      return returnResult(request, JSON.stringify({ error: "Could not fetch room capacity" }))
    }
  }

  async getOwnerUnreadMessages(request) {
    return returnResult(request, JSON.stringify({ unreadMessages: this.ownerUnread }), 200);
  }

  async getPubKeys(request) {
    return returnResult(request, JSON.stringify({ keys: this.visitors }), 200);
  }

  async acceptVisitor(request) {
    try {
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
    } catch (e) {
      console.log("Could not accept visitor: ", e);
      return returnResult(request, JSON.stringify({ error: "Could not accept visitor" }), 500);
    }
  }

  async lockRoom(request) {
    try {
      this.locked = true;
      for (let i = 0; i < this.visitors.length; i++) {
        if (this.accepted_requests.indexOf(this.visitors[i]) < 0 && this.join_requests.indexOf(this.visitors[i]) < 0) {
          this.join_requests.push(this.visitors[i]);
        }
      }
      this.storage.put('join_requests', JSON.stringify(this.join_requests));
      this.storage.put('locked', this.locked)
      return returnResult(request, JSON.stringify({ locked: this.locked }), 200);
    } catch (error) {
      console.log("Could not lock room: ", error);
      return returnResult(request, JSON.stringify({ error: "Could not restrict room" }), 500)
    }
  }

  async getJoinRequests(request) {
    try {
      return returnResult(request, JSON.stringify({ join_requests: this.join_requests }), 200);
    } catch (error) {
      return returnResult(request, JSON.stringify({ error: 'Could not get join requests' }), 500);
    }
  }

  async isRoomLocked(request) {
    try {
      return returnResult(request, JSON.stringify({ locked: this.locked }), 200);
    } catch (error) {
      console.log(error);
      return returnResult(request, JSON.stringify({ error: 'Could not get restricted status' }), 500);
    }
  }

  async setMOTD(request) {
    try {
      let data;
      data = await request.json();

      this.motd = data.motd;
      this.storage.put('motd', this.motd);
      return returnResult(request, JSON.stringify({ motd: this.motd }), 200);
    } catch (e) {
      console.log("Could not set message of the day: ", e)
      return returnResult(request, JSON.stringify({ error: "Could not set message of the day" }), 200);
    }
  }

  async ownerKeyRotation(request) {
    try {
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
    } catch (error) {
      console.log("Check for owner key rotation failed: ", error);
      return returnResult(request, JSON.stringify({
        success: false,
        error: '[ownerKeyRotation()] ' + error.message + '\n' + error.stack
      }), 200)
    }
  }

  async checkRotation(_timeout) {
    try {
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
    } catch (error) {
      console.log("Error while checking for owner key rotation: ", error)
      return false;
    }
  }


  // NOTE: current design limits this to 2^52 bytes, future limit will be 2^64 bytes
  roundSize(size, roundUp = true) {
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
  async handleNewStorage(request) {
    try {
      const { searchParams } = new URL(request.url);
      const size = this.roundSize(searchParams.get('size'));
      const storageLimit = this.storageLimit;
      if (size > storageLimit) return returnResult(request, JSON.stringify({ error: 'Not sufficient storage budget left in channel' }), 500);
      if (size > STORAGE_SIZE_MAX) return returnResult(request, JSON.stringify({ error: `Storage size too large (max ${STORAGE_SIZE_MAX} bytes)` }), 500);
      this.storageLimit = storageLimit - size;
      this.storage.put('storageLimit', this.storageLimit);
      const token_buffer = crypto.getRandomValues(new Uint8Array(48)).buffer;
      const token_hash_buffer = await crypto.subtle.digest('SHA-256', token_buffer)
      const token_hash = this.arrayBufferToBase64(token_hash_buffer);
      const kv_data = { used: false, size: size };
      const kv_resp = await this.env.LEDGER_NAMESPACE.put(token_hash, JSON.stringify(kv_data));
      const encrypted_token_id = this.arrayBufferToBase64(await crypto.subtle.encrypt({ name: "RSA-OAEP" }, this.ledgerKey, token_buffer));
      const hashed_room_id = this.arrayBufferToBase64(await crypto.subtle.digest('SHA-256', (new TextEncoder).encode(this.room_id)));
      const token = { token_hash: token_hash, hashed_room_id: hashed_room_id, encrypted_token_id: encrypted_token_id };
      return returnResult(request, JSON.stringify(token), 200);
    } catch (error) {
      return returnResult(request, JSON.stringify({ error: '[handleNewStorage()] ' + error.message + '\n' + error.stack }), 500)
    }
  }

  async getStorageLimit(request) {
    try {
      return returnResult(request, JSON.stringify({ storageLimit: this.storageLimit }), 200);
    } catch (error) {
      return returnResult(request, JSON.stringify({ error: '[getCurrentStorage()] ' + error.message + '\n' + error.stack }), 500)
    }
  }


  async getMother(request) {
    try {
      return returnResult(request, JSON.stringify({ motherChannel: this.motherChannel }), 200);
    } catch (error) {
      return returnResult(request, JSON.stringify({ error: '[getMother()] ' + error.message + '\n' + error.stack }), 500)
    }
  }


  transferBudgetLegal(transferBudget) {
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
  async handleBuddRequest(request) {
    try {
      if (DEBUG2) console.log(request)
      const _secret = this.env.SERVER_SECRET;
      const { searchParams } = new URL(request.url);
      const targetChannel = searchParams.get('targetChannel');
      let transferBudget = this.roundSize(searchParams.get('transferBudget'));
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
          return returnResult(request, JSON.stringify({ error: '[budd()]: Authentication of transfer failed' }), 500, 50);
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
    } catch (error) {
      return returnResult(request, JSON.stringify({ error: '[handleBuddRequest()] failed (transfer may have been lost)' + error.message + '\n' + error.stack }), 500)
    }
  }


  async handleAdminDataRequest(request) {
    try {
      return returnResult(request, JSON.stringify({
        join_requests: this.join_requests,
        capacity: this.room_capacity
      }), 200);
    } catch (error) {
      return returnResult(request, JSON.stringify({ error: 'Could not get admin data' }), 500);
    }
  }

  async verifyCookie(request) {

    // Parse cookie code from https://stackoverflow.com/questions/51812422/node-js-how-can-i-get-cookie-value-by-cookie-name-from-request

    try {
      let cookies = {};
      request.headers.has('cookie') && request.headers.get('cookie').split(';').forEach(function (cookie) {
        let parts = cookie.match(/(.*?)=(.*)$/)
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
    } catch (error) {
      return false;
    }
  }

  async verifyAuthSign(request: Request): Promise<boolean> {
    try {
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
      let roomSignKey = await window.crypto.subtle.importKey("jwk", this.signKey, {
        name: "ECDH",
        namedCurve: "P-384"
      }, false, ["deriveKey"]);
      let verificationKey = await window.crypto.subtle.deriveKey(
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
      return await window.crypto.subtle.verify("HMAC", verificationKey, this.base64ToArrayBuffer(sign), new TextEncoder().encode(auth_parts[0]));
    } catch (error) {
      console.log("Error verifying (owner) signature", error.stack)
      return false;
    }
  }

  async verifySign(secretKey, sign, contents) {
    try {
      const _sign = this.base64ToArrayBuffer(decodeURIComponent(sign));
      const encoder = new TextEncoder();
      const encoded = encoder.encode(contents);
      let verified = await crypto.subtle.verify(
        { name: 'ECDSA', hash: 'SHA-256' },
        secretKey,
        _sign,
        encoded
      );
      return verified;
    } catch (e) {
      return false;
    }
  }

  async sign(secretKey, contents) {
    try {
      const encoder = new TextEncoder();
      const encoded = encoder.encode(contents);
      let sign;
      try {
        sign = await window.crypto.subtle.sign(
          'HMAC',
          secretKey,
          encoded
        );
        return encodeURIComponent(this.arrayBufferToBase64(sign));
      } catch (error) {
        // console.log(error);
        return { error: "Failed to sign content" };
      }
    } catch (error) {
      // console.log(error);
      return { error: '[sign() ]' + error.message + '\n' + error.stack };
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

  async convertToPem(keys) {
    let _keys = {};
    for (let key in keys) {
      try {
        if (keys[key] === null || keys[key] === "") {
          continue;
        }
        const keyType = key.split('_').slice(-1)[0];
        let val = typeof keys[key] === 'object' ? keys[key] : jsonParseWrapper(keys[key], 'L956');
        if (keyType === 'encryptionKey') {
          const cryptoKey = await crypto.subtle.importKey("jwk", val, {
            name: "AES-GCM",
            length: 256
          }, true, ['encrypt', 'decrypt']);
          const exported = await crypto.subtle.exportKey("raw", cryptoKey);
          _keys[key] = btoa(this.ab2str(exported));
        } else if (keyType === 'signKey') {
          const cryptoKey = await crypto.subtle.importKey("jwk", val, {
            name: "ECDH",
            namedCurve: "P-384"
          }, true, ['deriveKey']);
          const _pemKey = await this.exportPrivateCryptoKey(cryptoKey);
          _keys[key] = _pemKey;
        } else {
          const cryptoKey = await crypto.subtle.importKey("jwk", val, { name: "ECDH", namedCurve: "P-384" }, true, []);
          const _pemKey = await this.exportPublicCryptoKey(cryptoKey);
          _keys[key] = _pemKey;
        }
      } catch {
        _keys[key] = "ERROR"
      }
    }
    return _keys;
  }

  base64ToArrayBuffer(base64) {
    var binary_string = atob(base64);
    var len = binary_string.length;
    var bytes = new Uint8Array(len);
    for (var i = 0; i < len; i++) {
      bytes[i] = binary_string.charCodeAt(i);
    }
    return bytes.buffer;
  }

  arrayBufferToBase64(buffer) {
    // try {  // better to just throw the error

    let binary = '';
    const bytes = new Uint8Array(buffer);
    const len = bytes.byteLength;
    for (let i = 0; i < len; i++) {
      binary += String.fromCharCode(bytes[i]);
    }
    return btoa(binary);

    // }
    // catch (e) {
    //   console.log(e);
    //   return { error: e };
    // }
  }

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

  registerDevice(request) {
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

  async downloadAllData(request) {
    let storage = await this.storage.list();
    let data = {
      roomId: this.room_id,
      ownerKey: this.room_owner,
      encryptionKey: this.encryptionKey,
      guestKey: this.verified_guest,
      signKey: this.signKey
    };
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
    const corsHeaders = {
      "Access-Control-Allow-Methods": "POST, OPTIONS, GET",
      "Access-Control-Allow-Headers": "Content-Type, authorization",
      "Access-Control-Allow-Origin": request.headers.get("Origin")
    }
    return new Response(dataBlob, { status: 200, headers: corsHeaders });
  }

  // used to create channels (from scratch) (or upload from backup)
  // TODO: can this overwrite a channel on the server?  is that ok?  (even if it's the owner)
  async uploadData(request) {
    if (DEBUG) { console.log("== uploadData() =="); if (DEBUG2) console.log(request); }
    let _secret = this.env.SERVER_SECRET;
    let data = await request.arrayBuffer();
    let jsonString = new TextDecoder().decode(data);
    let jsonData = jsonParseWrapper(jsonString, 'L1416');
    let roomInitialized = !(this.room_owner === "" || this.room_owner === null);
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
      this.storage.put("personalRoom", true);
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
      }, 200));
    }
  }

  async authorizeRoom(request) {
    let _secret = this.env.SERVER_SECRET;
    let jsonData = await request.json();
    let requestAuthorized = jsonData.hasOwnProperty("SERVER_SECRET") && jsonData["SERVER_SECRET"] === _secret;
    if (requestAuthorized) {
      for (let key in jsonData) {
        await this.storage.put("room_owner", jsonData["ownerKey"]);
      }
      this.personalRoom = true;
      this.storage.put("personalRoom", true);
      this.room_owner = jsonData["room_owner"];
      return returnResult(request, JSON.stringify({ success: true }), 200);
    } else {
      return returnResult(request, JSON.stringify({ success: false, error: "Server secret did not match" }, 200));
    }
  }
}
