
// .... currently getLastMessageTimes is disabled ... once it's back we will need this
// async function lastTimeStamp(room_id: SBChannelId, env: EnvType) {
//   let list_response = await env.MESSAGES_NAMESPACE.list({ "prefix": room_id });
//   let message_keys: any = list_response.keys.map((res) => {
//     return res.name
//   });
//   if (message_keys.length === 0) return '0'
//   while (!list_response.list_complete) {
//     list_response = await env.MESSAGES_NAMESPACE.list({ "cursor": list_response.cursor })
//     message_keys = [...message_keys, list_response.keys];
//   }
//   return message_keys[message_keys.length - 1].slice(room_id.length);
// }



/* old ChannelServer properties */


  /* 'ownerUnread'         */ // ownerUnread: number = 0;
  /* 'joinRequests'        */ // joinRequests: Array<SBUserId> = [];
  /* 'acceptedRequests'    */ // acceptedRequests: Array<SBUserId> = [];
  /* 'encryptedLockedKeys' */ // encryptedLockedKeys: Map<SBUserId, string> = new Map();


  // env: EnvType;

  // #channelKeys?: SBChannelKeys // ... being deprectated ...

  // #channelKeyStrings?: ChannelKeyStrings

  // // 'room_owner' is old format, was kept as a (json) string of public key JWK of owner
  // room_owner_old_format: string | null = null; 

  // new format is a the public user id of owner (which is public key of owner/creator)
  // (for now we will maintain both formats in the key value stores, but in-memory eg DO
  //  objects work with roomOwnerId and newer clients need to understand that)
  // ownerId?: SBUserId


  // verified_guest: string = '';
  // visitors: Array<JsonWebKey> = [];
  // join_requests: Array<JsonWebKey> = [];
  // accepted_requests: Array<JsonWebKey> = [];
  // lockedKeys: Array<JsonWebKey> = []; // tracks history of lock keys
  // room_capacity: number = 20;
  // motd: string = '';
  // ledgerKey: CryptoKey | null = null;
  // personalRoom: boolean = false; // new protocol - all rooms are "personal" (this is an SSO holdover)


      // s += `verified_guest: ${this.verified_guest}\n`;
    // s += `ownerUnread: ${this.ownerUnread}\n`;
    // s += `motd: ${this.motd}\n`;
    // s += `personalRoom: ${this.personalRoom}\n`;
    // s += `messagesCursor: ${this.messagesCursor}\n`;
    // s += `lastTimestamp: ${this.lastTimestamp}\n`;
    // s += `visitors: ${this.visitors}\n`;
    // s += `join_requests: ${this.join_requests}\n`;
    // s += `accepted_requests: ${this.accepted_requests}\n`;
    // s += `lockedKeys: ${this.lockedKeys}\n`;


    // this.ownerUnread = Number(await this.#getKey('ownerUnread')) || 0;
    // this.joinRequests = jsonParseWrapper(await this.#getKey('joinRequests') || JSON.stringify([]), 'L433');
    // this.acceptedRequests = jsonParseWrapper(await this.#getKey('acceptedRequests') || JSON.stringify([]), 'L437');
    // this.encryptedLockedKeys = this.deserializeMap(await this.#getKey('encryptedLockedKeys') || '[]');



    /* old #initialize */
          // const _channelData: SBChannelData = jsonParseWrapper(await this.#getKey('channelData'), 'L409')
      // _sb_assert(_channelData, "ERROR: no channel data found in environment (fatal)")
      // _sb_assert(_channelData.channelId === channelId, "ERROR: channel data does not match channel ID (fatal)")

      // const ledgerKeyString = this.env.LEDGER_KEY; _sb_assert(ledgerKeyString, "ERROR: no ledger key found in environment (fatal)")
      // const ledgerKey = await crypto.subtle.importKey("jwk", jsonParseWrapper(ledgerKeyString, 'L424'), { name: "RSA-OAEP", hash: 'SHA-256' }, true, ["encrypt"])
      // this.ledgerKey = ledgerKey; // a bit quicker

      // TODO: test refactored lock
      // for (let i = 0; i < this.accepted_requests.length; i++)
      //   // this.lockedKeys[this.accepted_requests[i]] = await storage.get(this.accepted_requests[i]);
      //   this.lockedKeys[this.accepted_requests[i].x!] = await this.storage.get(this.accepted_requests[i]);



      // OLD version:

      // #setupSession(session: SessionType, msg: any) {
      //   const webSocket = session.webSocket;
      //   try {
    
      //     // // The first message the client sends is the user info message with their pubKey.
      //     // // Save it into their session object and in the visitor list.
      //     // if (!this.#channelKeys) {
      //     //   webSocket.close(4000, "This room does not have an owner, or the owner has not enabled it. You cannot interact with it yet.");
      //     //   if (DEBUG) console.log("no owner - closing")
      //     //   return;
      //     // }
      //     // if (data.pem) {
      //     //   webSocket.send(JSON.stringify({ error: "ERROR: PEM formats no longer used" }));
      //     //   return;
      //     // }
    
      //     const data = jsonParseWrapper(msg.data.toString(), 'L610');
    
      //     if (!data.userId) {
      //       // todo: actually in 2.0 jslib, every message includes userId
      //       webSocket.send(JSON.stringify({ error: "ERROR: First message needs to contain pubKey" }));
      //       return;
      //     }
      //     // const _name: JsonWebKey = jsonParseWrapper(data.name, 'L578');
      //     const userId: SBUserId = (data as any).userId
      //     // const isPreviousVisitor = sbCrypto.lookupKey(_name, this.visitors) >= 0;
      //     const isPreviousVisitor = this.visitors.includes(userId)
      //     // const isAccepted = sbCrypto.lookupKey(_name, this.accepted_requests) >= 0;
      //     const isAccepted = this.acceptedRequests.includes(userId)
      //     if (!isPreviousVisitor && this.visitors.length >= this.channelCapacity) {
      //       webSocket.close(4000, 'ERROR: The room is not accepting any more visitors.');
      //       return;
      //     }
      //     if (!isPreviousVisitor) {
      //       this.visitors.push(jsonParseWrapper(data.name, 'L594'));
      //       this.storage.put('visitors', JSON.stringify(this.visitors))
      //     }
      //     if (this.locked) {
      //       if (!isAccepted && !isPreviousVisitor) {
      //         this.joinRequests.push(data.name);
      //         this.storage.put('join_requests', JSON.stringify(this.joinRequests));
      //         // TODO ok did we not reject before? when/where should we signal/enforce?
      //         webSocket.close(4000, "ERROR: this is a locked room and you haven't yet been accepted by owner.");
      //         return;
      //       } else {
      //         // this is not done globally, but per visitor (with getchannelkeys)
      //         if (DEBUG)
      //           console.log("visitor is accepted to a locked room, will be provided with get channel keys")
    
      //         //   // TODO: this mechanism needs testing
      //         //   // const encrypted_key = this.lockedKeys[sbCrypto.lookupKey(_name, this.lockedKeys)];
      //         //   const encryptedLockedKey = this.encryptedLockedKeys.get(data.name) || null;
      //         //   if (encryptedLockedKey)
      //         //     this.#channelKeys!.encryptedLockedKey = encryptedLockedKey;
      //         // }
      //       }
      //     }
      //     // session.name = data.name;
      //     session.name = userId;
      //     webSocket.send(JSON.stringify({
      //       ready: true,
      //       // keys: this.#channelKeys,
      //       // // encryptionKey: this.#channelKeyStrings!.encryptionKey,
      //       // // ownerKey: this.#channelKeyStrings!.ownerKey,
      //       // // signKey: this.#channelKeyStrings!.signKey,
      //       // ownerPublicKey: this.#channelKeys!.ownerPublicKey,
      //       // channelSignKey: this.#channelKeys!.channelSignKey,
      //       // // TODO: guest key?
      //       // motd: this.motd, roomLocked: this.locked
      //     }));
      //     session.channelId = "" + data.channelId;
      //     // Note that we've now received the user info message for this session
      //     session.receivedUserInfo = true;
      //   } catch (err: any) {
      //     webSocket.send(JSON.stringify({ error: "ERROR: problem setting up session: " + err.message + '\n' + err.stack }));
      //     return;
      //   }
      // }