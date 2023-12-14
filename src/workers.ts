/// <reference types="@cloudflare/workers-types" />

/*
 * this file should be the same between channel and storage server
 */

import { DEBUG, DEBUG2 } from './env'

// internal - handle assertions
export function _sb_assert(val: unknown, msg: string) {
    if (!(val)) {
        const m = `<< SB assertion error: ${msg} >>`;
        throw new Error(m);
    }
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

export function returnResult(request: Request, contents: any, status: ResponseCode, delay = 0) {
    const corsHeaders = _corsHeaders(request, "application/json; charset=utf-8");
    return new Promise<Response>((resolve) => {
        setTimeout(() => {
            if (DEBUG2) console.log("++++ returnResult() contents:", contents, "status:", status)
            resolve(new Response(contents, { status: status, headers: corsHeaders }));
        }, delay);
    });
}

export function returnBinaryResult(request: Request, payload: BodyInit) {
    const corsHeaders = _corsHeaders(request, "application/octet-stream");
    return new Response(payload, { status: 200, headers: corsHeaders });
}

export function returnError(_request: Request, errorString: string, status: ResponseCode, delay = 0) {
    if (DEBUG) console.log("**** ERROR: (status: " + status + ")\n" + errorString);
    if (!delay && ((status == 401) || (status == 403))) delay = 50; // delay if auth-related
    return returnResult(_request, `{ "error": "${errorString}" }`, status);
}

// this handles UNEXPECTED errors
export async function handleErrors(request: Request, func: () => Promise<Response>) {
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