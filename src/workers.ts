/// <reference types="@cloudflare/workers-types" />

/*
 * this file should be the same between channel and storage server
 */

import { assemblePayload, SBError } from 'snackabra';
import { NEW_CHANNEL_MINIMUM_BUDGET as _NEW_CHANNEL_MINIMUM_BUDGET } from 'snackabra'


// also exported to 'workers.ts'
export var dbg = {
    DEBUG: false,
    DEBUG2: false,
    LOG_ERRORS: true,
    myOrigin: '',     // tracks our origin as well
  }


/**
 * API calls are in one of two forms:
 * 
 * ::
 * 
 *     /api/v2/<api_call>/
 *     /api/v2/channel/<id>/<api_call>/
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
 *     /api/v2/channel/<id>/websocket
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
 *     /api/v2/info                 : channel server info (only API that is json)
 *     /api/v2/getLastMessageTimes  : queries multiple channels for last message timestamp (disabled)
 *
 *     NOTE: all channel api endpoints are binary (payload)
 * 
 *     Channel API (synchronous)                : [O] means [Owner] only
 *     /api/v2/channel/<ID>/acceptVisitor       : [O]
 *     /api/v2/channel/<ID>/budd                : [O] either creates a new channel or transfers storage
 *     /api/v2/channel/<ID>/downloadData        :     (v2 version not implemtened)
 *     /api/v2/channel/<ID>/getAdminData        : [O]
 *     /api/v2/channel/<ID>/getCapacity         : [O]
 *     /api/v2/channel/<ID>/getChannelKeys      :     get owner pub key, channel pub key
 *     /api/v2/channel/<ID>/getJoinRequests     : [O]
 *     /api/v2/channel/<ID>/getLatestTimestamp  :     in prefix format (v3 api)
 *     /api/v2/channel/<ID>/getMother           : [O]
 *     /api/v2/channel/<ID>/getPubKeys          :     returns Map<userId, pubKey>
 *     /api/v2/channel/<ID>/getStorageLimit     :     (under development)
 *     /api/v2/channel/<ID>/getStorageToken
 *     /api/v2/channel/<ID>/lockChannel         : [O]
 *     /api/v2/channel/<ID>/send
 *     /api/v2/channel/<ID>/setCapacity         : [O]
 *     /api/v2/channel/<ID>/uploadChannel       :     (v2/v3 not implemtened, will use shards)
 *     /api/v2/channel/<ID>/websocket           :     connect to channel socket (wss protocol)
 * 
 * The following are deprecated or disabled:
 * 
 * ::
 *
 *     /api/v2/notifications        : sign up for notifications (disabled)
 *     /api/v2/channel/<ID>/locked              : deprecated, use getAdminData
 *     /api/v2/channel/<ID>/motd                : [O]
 *     /api/v2/channel/<ID>/ownerKeyRotation    : [O] (deprecated)
 *     /api/v2/channel/<ID>/registerDevice      : (disabled)
 *     /api/v2/channel/<ID>/authorizeRoom       : (deprecated)
 *     /api/v2/channel/<ID>/oldMessages         : deprecated
 *     /api/v2/channel/<ID>/ownerUnread         : deprecated
 *     /api/v2/channel/<ID>/postPubKey          : deprecated
 * 
 */

const _STORAGE_SIZE_UNIT = 4096 // 4KB

export const serverConstants = {
    // minimum unt of storage
    STORAGE_SIZE_UNIT: _STORAGE_SIZE_UNIT,

    // Currently minimum (raw) storage is set to 32KB. This will not
    // be LOWERED, but future design changes may RAISE that. 
    STORAGE_SIZE_MIN: 8 * _STORAGE_SIZE_UNIT,

    // Current maximum (raw) storage is set to 32MB. This may change.
    // Note that this is for SHARDS not CHANNEL
    STORAGE_SIZE_MAX: 8192 * _STORAGE_SIZE_UNIT,

    // minimum when creating (budding) a new channel
    NEW_CHANNEL_MINIMUM_BUDGET: _NEW_CHANNEL_MINIMUM_BUDGET,

    // // new channel budget (bootstrap) is 3 GB (about $1)
    // NEW_CHANNEL_BUDGET: 3 * 1024 * 1024 * 1024, // 3 GB

    // sanity check - set a max at one petabyte (2^50) .. at a time
    MAX_BUDGET_TRANSFER: 1024 * 1024 * 1024 * 1024 * 1024, // 1 PB

    // see discussion in jslib
    MAX_SB_BODY_SIZE: 64 * 1024,

    // maximum number of (perma) messages kept in KV format; beyond this,
    // messages are shardified. note that current CF hard limit is 1000.
    MAX_MESSAGE_SET_SIZE: 100, // if this is lower than 1000, we're testing
}

// used by both storage and channel servers to create 'key' into IMAGES KV
// comment: 'Pages' are stored from channel server, and have type 'G'
export function genKey(id: string, type: string = '_') {
    _sb_assert(type.length === 1, "genKey() called with type length != 1")
    const key = "____" + type + "__" + id + "______"
    if (dbg.DEBUG2) console.log(`genKey(): '${key}'`)
    return key
}

// same but only constructs prefix, eg, skips tail "_" stuff
export function genKeyPrefix(prefix: string, type: string = '_') {
    _sb_assert(type.length === 1, "genKeyPrefix() called with type length != 1")
    const key = "____" + type + "__" + prefix
    if (dbg.DEBUG2) console.log(`genKey(): '${key}'`)
    // we safeguard here by never even constructing from anything shorter than six characters
    // (eg even with base32 we do not go below 30 bits of identification)
    if (key.length < 6) throw new SBError("genKeyPrefix() called with prefix length < 6")
    return key
}

export const serverApiCosts = {
    // multiplier of cost of storage on channel vs. storage server
    // (this includes Pages)
    CHANNEL_STORAGE_MULTIPLIER: 8.0,
    CHANNEL_STORAGE_MULTIPLIER_TTL_ZERO: 1/8.0 // upwards 1/100th cost of storing
}

// internal - handle assertions
export function _sb_assert(val: unknown, msg: string) {
    if (!(val)) {
        const m = `<< SB assertion error: ${msg} >>`;
        throw new Error(m);
    }
}

// appends one to the other
export function _appendBuffer(buffer1: Uint8Array | ArrayBuffer, buffer2: Uint8Array | ArrayBuffer): ArrayBuffer {
    const tmp = new Uint8Array(buffer1.byteLength + buffer2.byteLength);
    tmp.set(new Uint8Array(buffer1), 0);
    tmp.set(new Uint8Array(buffer2), buffer1.byteLength);
    return tmp.buffer;
}

// list of MIME types that are considered "text-like", which a Page retrieval
// will attempt to decode as text
export const textLikeMimeTypes: Set<string> = new Set([
    // Textual Data
    "text/plain",
    "text/html",
    "text/css",
    "text/javascript", // Note: application/javascript is more correct for JS
    "text/xml",
    "text/csv",

    // Application Data (often textual in nature)
    "application/json",
    "application/javascript", // More correct MIME type for JavaScript
    "application/xml",
    "application/xhtml+xml",
    "application/rss+xml",
    "application/atom+xml",

    // Markup Languages
    "image/svg+xml",
]);

// Example function to check if a MIME type is considered "text-like"
function isTextLikeMimeType(mimeType: string): boolean {
    return textLikeMimeTypes.has(mimeType);
}

// Example usage
console.log(isTextLikeMimeType("text/html")); // true
console.log(isTextLikeMimeType("application/json")); // true
console.log(isTextLikeMimeType("image/jpeg")); // false


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
export type ResponseCode = 101 | 200 | 400 | 401 | 403 | 404 | 405 | 413 | 418 | 429 | 500 | 501 | 507;
const SEP = '='.repeat(60) + '\n'

function _headers(request: Request, contentType: string | null, immutable: boolean = false): HeadersInit {
    let corsHeaders: HeadersInit = {
        "Access-Control-Allow-Methods": "POST, OPTIONS, GET",
        "Access-Control-Allow-Headers": "Content-Type, authorization",
        "Access-Control-Allow-Credentials": "true",
        "Access-Control-Allow-Origin": request.headers.get("Origin") ?? "*",
    };
    if (contentType)
        corsHeaders = { ...corsHeaders, "Content-Type": contentType };
    if (immutable) {
        corsHeaders = { ...corsHeaders, "Cache-Control": "public, max-age=31536000, immutable" };
    }
    if (dbg.DEBUG2) console.log('++++++++++++ HEADERS +++++++++++++\n\n', corsHeaders);
    return corsHeaders;
}

export interface ReturnOptions {
    status?: ResponseCode,
    delay?: number,
    headers?: HeadersInit,
    type?: string // MIME type, if omitted defaults to 'sb384payloadV3' eg payload/octet-stream
}

/**
 * General return result function. Defaults to 200 (OK) and no delay. Contents
 * are packaged as a payload, unless type indicates otherwise.
 */
export function returnResult(request: Request, contents: any = null, options: ReturnOptions = {}) {
    const status = options.status || 200, delay = options.delay || 0;
    if (!options.type || options.type === 'sb384payloadV3') contents = assemblePayload(contents);
    let type = options.type || "application/octet-stream";
    if (type === 'sb384payloadV3') type = "application/octet-stream";
    let headers = _headers(request, type);
    if (options.headers) headers = { ...headers, ...options.headers };
    return new Promise<Response>((resolve) => {
        setTimeout(() => {
            if (dbg.DEBUG2) console.log("++++ returnResult() contents:", contents, "status:", status)
            resolve(new Response(contents, { status: status, headers: headers }));
        }, delay);
    });
}

/**
 * Returns a result as a JSON object. Defaults to 200 (OK) and no delay.
 * Generally only used for API endpoints that might be called from non-SB384
 * code, such as mirrors or proxy servers (eg calling '/api/v2/info').
 */
export function returnResultJson(request: Request, contents: any, status: ResponseCode = 200, delay = 0) {
    const corsHeaders = _headers(request, "application/json; charset=utf-8");
    return new Promise<Response>((resolve) => {
        setTimeout(() => {
            const json = JSON.stringify(contents);
            if (dbg.DEBUG) console.log(
                SEP, `++++ returnResult() - status '${status}':\n`,
                SEP, 'contents:\n', contents, '\n',
                SEP, 'json:\n', json, '\n', SEP)
            resolve(new Response(json, { status: status, headers: corsHeaders }));
        }, delay);
    });
}

/**
 * Returns a minimal success result. Defaults to 200 (OK) and no delay.
 */
export function returnSuccess(request: Request) {
    return returnResultJson(request, { success: true });
}

// if there's an Etag match for a requested resource, return 304
export function return304(request: Request, hash: string) {
    let corsHeaders = _headers(request, null); // no content type
    // add hash as Etag
    corsHeaders = { ...corsHeaders, "Etag": hash };
    return new Response(null, { status: 304 });
}

/**
 * Slightly different from returnResult() in that it does not assemble the
 * payload but just passes on payload. Defaults to 200 (OK) and no delay.
 */
export function returnBinaryResult(request: Request, payload: BodyInit, headers?: HeadersInit) {
    let corsHeaders = _headers(request, "application/octet-stream");
    // if we have a list of headers, then we merge with corsHeaders
    if (headers) {
        corsHeaders = { ...corsHeaders, ...headers };
    }
    return new Response(payload, { status: 200, headers: corsHeaders });
}

/**
 * Simple error response. Defaults to 500 (Internal Server Error) and no delay.
 * Any auth-related errors (eg 401 and 403) will be delayed by 50ms.
 */
export function returnError(_request: Request, errorString: string, status: ResponseCode = 500, delay = 0) {
    console.log("**** ERROR: (status: " + status + ")\n" + errorString);
    if (!delay && ((status == 401) || (status == 403))) delay = 50; // delay if auth-related
    return returnResultJson(_request, { success: false, error: errorString }, status);
}

// this handles UNEXPECTED errors
export async function handleErrors(request: Request, func: () => Promise<Response>) {
    try {
        return await func();
    } catch (err: any) {
        if (err instanceof Error) {
            if (request.headers.get("Upgrade") == "websocket") {
                const [_client, server] = Object.values(new WebSocketPair());
                if (!server) return returnError(request, "Missing server from client/server of websocket (?)")
                if ((server as any).accept) {
                    (server as any).accept(); // CF typing override (TODO: report this)
                    server.send(JSON.stringify({ error: '[handleErrors()] ' + err.message + '\n' + err.stack }));
                    server.close(1011, "Uncaught exception during session setup");
                    console.log("webSocket close (error)")
                }
                return returnResult(request, null, { status: 101 });
            } else {
                return returnResult(request, err.stack, { status: 500 })
            }
        } else {
            return returnError(request, "Unknown error type (?) in top level", 500);
        }
    }
}

import type { EnvType } from './env'
import { handleApiRequest } from './index'

export async function serverFetch(request: Request, env: EnvType) {
    if (dbg.DEBUG) console.log("serverFetch() called with url:", request.url)
    return await handleErrors(request, async () => {
        if (request.method == "OPTIONS")
            return returnResult(request);
        const path = (new URL(request.url)).pathname.slice(1).split('/');
        if (dbg.DEBUG2) console.log("serverFetch() path:", path)
        if ((path.length >= 1) && (path[0] === 'api') && (path[1] == 'v2'))
            return handleApiRequest(path.slice(2), request, env);
        else
            return returnError(request, "Not found (must give API endpoint '/api/v2/...')", 404)
    });
}

export default {
    async fetch(request: Request, env: EnvType) {
        // note: this will only toggle these values in this file
        dbg.DEBUG = env.DEBUG_ON === true
        dbg.DEBUG2 = env.VERBOSE_ON === true
        dbg.LOG_ERRORS = env.LOG_ERRORS === true
        if (!dbg.myOrigin) dbg.myOrigin = new URL(request.url).origin

        if (dbg.DEBUG) {
            const msg = `==== [${request.method}] Fetch called: ${request.url}`;
            console.log(
                `\n${'='.repeat(Math.min(msg.length, 120))}` +
                `\n${msg}` +
                `\n${'='.repeat(Math.min(msg.length, 120))}`
            );
            if (dbg.DEBUG2) console.log(request.headers);
        }
        return await serverFetch(request, env);
    }
}


import { extractPayload, base62ToArrayBuffer, validate_ChannelApiBody, ChannelApiBody } from 'snackabra'


/**
 * Extracts the apiBody from the request. If the request is a binary
 * request, it will extract the payload and then extract the apiBody
 * from the payload. If the request is a GET request, it will extract
 * the apiBody from the query string.
 */
export async function processApiBody(request: Request): Promise<Response | ChannelApiBody> {
    const contentType = request.headers.get('content-type');
    let _apiBody: ChannelApiBody | null = null;
    if (contentType?.includes("application/octet-stream")) {
        _apiBody = extractPayload(await request.arrayBuffer()).payload;
    } else {
        const apiBodyBuf62 = new URL(request.url).searchParams.get('apiBody');
        const apiBodyBuf = apiBodyBuf62 ? base62ToArrayBuffer(apiBodyBuf62) : null;
        _apiBody = apiBodyBuf ? extractPayload(apiBodyBuf).payload : null
    }
    if (!_apiBody) return returnError(request, "Channel API - cannot find and/or parse apiBody", 400);
    const apiBody = validate_ChannelApiBody(_apiBody); // will throw if anything wrong
    // if there's an apiPayloadBuf, we need to extract it
    if (apiBody.apiPayload) return returnError(request, "[fetch]: do not provide 'apiPayload'", 400)
    if (apiBody.apiPayloadBuf) {
        apiBody.apiPayload = extractPayload(apiBody.apiPayloadBuf).payload
        if (!apiBody.apiPayload) return returnError(request, "[fetch]: cannot extract from provided apiPayloadBuf", 400)
    }
    return apiBody;
}

// used consistently with delay 50 throughout for any fail conditions to avoid providing any info
export const ANONYMOUS_CANNOT_CONNECT_MSG = "No such channel or shard, or you are not authorized."

import { SBStorageToken, validate_SBStorageToken, jsonParseWrapper } from 'snackabra'

function delay(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
  
// fetches the server's point of view on a storage token; any issues an it returns null
// server token is stored in JSON to facilitate using dashboard view of KV
export async function getServerStorageToken(hash: string, env: EnvType): Promise<SBStorageToken | null> {
  if (dbg.DEBUG) console.log(`[getServerStorageToken()]: looking up token ${hash} in ledger`)
  try {
    var _storage_token = await env.LEDGER_NAMESPACE.get(hash);
    if (!_storage_token) {
        if (dbg.DEBUG) console.log(`[getServerStorageToken()]: could not find ${hash} in ledger, will wait a bit and retry`)
        await delay(1000);
        _storage_token = await env.LEDGER_NAMESPACE.get(hash);
        if (!_storage_token) {
            if (dbg.DEBUG) console.log(`[getServerStorageToken()]: cannot find token ${hash} in ledger`, _storage_token)
            if (dbg.DEBUG) console.log(env.LEDGER_NAMESPACE)
            return null;
        } else {
            if (dbg.DEBUG) console.log(`[getServerStorageToken()]: found token ${hash} on second try:`, _storage_token)
        }
    }
    if (dbg.DEBUG) console.log(`[getServerStorageToken()]: found token ${hash} in ledger:`, _storage_token)
    const _ledger_resp = jsonParseWrapper(_storage_token, 'L1090');
    return validate_SBStorageToken(_ledger_resp)
  } catch (error: any) {
    if (dbg.DEBUG) console.log(`[getServerStorageToken()]: issues with token ${hash} in ledger:`, error.message)
    return null;
  }
}


// /// <reference types="@cloudflare/workers-types" />

// /*
//  * this file should be the same between channel and storage server
//  */

// import { dbg.DEBUG, dbg.DEBUG2 } from './env'

// // internal - handle assertions
// export function _sb_assert(val: unknown, msg: string) {
//     if (!(val)) {
//         const m = `<< SB assertion error: ${msg} >>`;
//         throw new Error(m);
//     }
// }

// // Reminder of response codes we use:
// //
// // 101: Switching Protocols (downgrade error)
// // 200: OK
// // 400: Bad Request
// // 401: Unauthorized
// // 403: Forbidden
// // 404: Not Found
// // 405: Method Not Allowed
// // 413: Payload Too Large
// // 418: I'm a teapot
// // 429: Too Many Requests
// // 500: Internal Server Error
// // 501: Not Implemented
// // 507: Insufficient Storage (WebDAV/RFC4918)
// //
// export type ResponseCode = 101 | 200 | 400 | 401 | 403 | 404 | 405 | 413 | 418 | 429 | 500 | 501 | 507;

// function _corsHeaders(request: Request, contentType: string) {
//     const corsHeaders = {
//         "Access-Control-Allow-Methods": "POST, OPTIONS, GET",
//         "Access-Control-Allow-Headers": "Content-Type, authorization",
//         "Access-Control-Allow-Credentials": "true",
//         "Access-Control-Allow-Origin": request.headers.get("Origin") ?? "*",
//         "Content-Type": contentType,
//     }
//     if (dbg.DEBUG2) console.log('++++++++++++ HEADERS +++++++++++++\n\n', corsHeaders)
//     return corsHeaders;
// }

// export function returnResult(request: Request, contents: any, status: ResponseCode = 200, delay = 0) {
//     const corsHeaders = _corsHeaders(request, "application/json; charset=utf-8");
//     return new Promise<Response>((resolve) => {
//         setTimeout(() => {
//             if (dbg.DEBUG2) console.log("++++ returnResult() contents:", contents, "status:", status)
//             resolve(new Response(contents, { status: status, headers: corsHeaders }));
//         }, delay);
//     });
// }

// export function returnBinaryResult(request: Request, payload: BodyInit) {
//     const corsHeaders = _corsHeaders(request, "application/octet-stream");
//     return new Response(payload, { status: 200, headers: corsHeaders });
// }

// export function returnError(_request: Request, errorString: string, status: ResponseCode = 500, delay = 0) {
//     if (dbg.DEBUG) console.log("**** ERROR (status: " + status + "):\n'" + errorString +"'");
//     if (!delay && ((status === 401) || (status === 403) || (status === 404))) delay = 50; // delay if auth-related
//     return returnResult(_request, `{ "error": "${errorString}" }`, status);
// }

// // this handles UNEXPECTED errors
// export async function handleErrors(request: Request, func: () => Promise<Response>) {
//     try {
//         return await func();
//     } catch (err: any) {
//         if (err instanceof Error) {
//             if (request.headers.get("Upgrade") == "websocket") {
//                 const [_client, server] = Object.values(new WebSocketPair());
//                 if ((server as any).accept) {
//                     (server as any).accept(); // CF typing override (TODO: report this)
//                     server.send(JSON.stringify({ error: '[handleErrors()] ' + err.message + '\n' + err.stack }));
//                     server.close(1011, "Uncaught exception during session setup");
//                     console.log("webSocket close (error)")
//                 }
//                 return returnResult(request, null, 101);
//             } else {
//                 return returnResult(request, err.stack, 500)
//             }
//         } else {
//             return returnError(request, "Unknown error type (?) in top level", 500);
//         }
//     }
// }