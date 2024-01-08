/// <reference types="@cloudflare/workers-types" />

/*
 * this file should be the same between channel and storage server
 */

import { assemblePayload } from 'snackabra';
import { DEBUG, DEBUG2 } from './env'
import { NEW_CHANNEL_MINIMUM_BUDGET as _NEW_CHANNEL_MINIMUM_BUDGET } from 'snackabra'

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
 *     /api/v2/channel/<ID>/create              : New: create with storage token
 *     /api/v2/channel/<ID>/websocket           : connect to channel socket (wss protocol)
 *     /api/v2/channel/<ID>/oldMessages
 *     /api/v2/channel/<ID>/updateRoomCapacity  : [O]
 *     /api/v2/channel/<ID>/budd                : [O]
 *     /api/v2/channel/<ID>/getAdminData        : [O]
 *     /api/v2/channel/<ID>/getJoinRequests     : [O]
 *     /api/v2/channel/<ID>/getChannelKeys      : New: get owner pub key, channel pub key
 *     /api/v2/channel/<ID>/getMother           : [O]
 *     /api/v2/channel/<ID>/getRoomCapacity     : [O]
 *     /api/v2/channel/<ID>/getStorageLimit     : ToDo: per-userId storage limit system (until then shared)
 *     /api/v2/channel/<ID>/acceptVisitor       : [O]
 *     /api/v2/channel/<ID>/channelLocked
 *     /api/v2/channel/<ID>/ownerUnread         : [O]
 *     /api/v2/channel/<ID>/storageRequest
 *     /api/v2/channel/<ID>/downloadData
 *     /api/v2/channel/<ID>/uploadChannel       : (admin only or with budget channel provided)
 *     /api/v2/channel/<ID>/postPubKey
 * 
 * The following are in the process of being reviewed / refactored / deprecated:
 * 
 * ::
 *
 *     /api/v2/notifications        : sign up for notifications (disabled)
 *     /api/v2/channel/<ID>/roomLocked          : (alias for /channelLocked)
 *     /api/v2/channel/<ID>/lockRoom            : [O]
 *     /api/v2/channel/<ID>/motd                : [O]
 *     /api/v2/channel/<ID>/ownerKeyRotation    : [O] (deprecated)
 *     /api/v2/channel/<ID>/registerDevice      : (disabled)
 *     /api/v2/channel/<ID>/uploadRoom          : (admin only or with budget channel provided)
 *     /api/v2/channel/<ID>/authorizeRoom       : (admin only)
 *     /api/v2/channel/<ID>/getPubKeys          : [O]
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
    STORAGE_SIZE_MAX: 8192 * _STORAGE_SIZE_UNIT,

    // minimum when creating (budding) a new channel
    NEW_CHANNEL_MINIMUM_BUDGET: _NEW_CHANNEL_MINIMUM_BUDGET,

    // new channel budget (bootstrap) is 3 GB (about $1)
    NEW_CHANNEL_BUDGET: 3 * 1024 * 1024 * 1024, // 3 GB

    // sanity check - set a max at one petabyte (2^50)
    MAX_BUDGET_TRANSFER: 1024 * 1024 * 1024 * 1024 * 1024, // 1 PB
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

function _corsHeaders(request: Request, contentType: string) {
    const corsHeaders = {
        "Access-Control-Allow-Methods": "POST, OPTIONS, GET",
        "Access-Control-Allow-Headers": "Content-Type, authorization",
        "Access-Control-Allow-Credentials": "true",
        "Access-Control-Allow-Origin": request.headers.get("Origin") ?? "*",
        "Content-Type": contentType,
    }
    if (DEBUG2) console.log('++++++++++++ HEADERS +++++++++++++\n\n', corsHeaders)
    return corsHeaders;
}

export function returnResult(request: Request, contents: any = null, status: ResponseCode = 200, delay = 0) {
    const corsHeaders = _corsHeaders(request, "application/octet-stream");
    return new Promise<Response>((resolve) => {
        setTimeout(() => {
            if (DEBUG2) console.log("++++ returnResult() contents:", contents, "status:", status)
            if (contents) contents = assemblePayload(contents);
            resolve(new Response(contents, { status: status, headers: corsHeaders }));
        }, delay);
    });
}

export function returnResultJson(request: Request, contents: any, status: ResponseCode = 200, delay = 0) {
    const corsHeaders = _corsHeaders(request, "application/json; charset=utf-8");
    return new Promise<Response>((resolve) => {
        setTimeout(() => {
            const json = JSON.stringify(contents);
            if (DEBUG) console.log(
                SEP, `++++ returnResult() - status '${status}':\n`,
                SEP, 'contents:\n', contents, '\n',
                SEP, 'json:\n', json, '\n', SEP)
            resolve(new Response(json, { status: status, headers: corsHeaders }));
        }, delay);
    });
}

export function returnSuccess(request: Request) {
    return returnResultJson(request, { success: true });
}

export function returnBinaryResult(request: Request, payload: BodyInit) {
    const corsHeaders = _corsHeaders(request, "application/octet-stream");
    return new Response(payload, { status: 200, headers: corsHeaders });
}

export function returnError(_request: Request, errorString: string, status: ResponseCode = 500, delay = 0) {
    if (DEBUG) console.log("**** ERROR: (status: " + status + ")\n" + errorString);
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
                return returnResult(request, null, 101);
            } else {
                return returnResult(request, err.stack, 500)
            }
        } else {
            return returnError(request, "Unknown error type (?) in top level", 500);
        }
    }
}