/// <reference lib="dom" />
import fetch, { RequestInit, Response } from 'node-fetch';
import AbortController from 'abort-controller';
import FormData from 'form-data';
import { DictStrStr } from './types';
import { AccessDenied, ServerSideError } from './errors';
import { getQueryURL } from './util';
import { Readable } from 'stream';

export class HTTPResponseError extends Error {
    response: Response;
    constructor(response: Response) {
        super(
            `HTTP Error Response: ${response.status} ${response.statusText}`
        );
        this.response = response;
    }
}

// ====== Timeout ======

async function sleep(time: number) {
    return new Promise((resolve) => setTimeout(resolve, time));
}

// ====== Retry ======
export type RetryOptions = {
    attempts: number;
    codes: number[];
    delay: number;
};

export const defaultRetryOptions: RetryOptions = {
    attempts: 20,
    codes: [403, 404, 500],
    delay: 5,
};

export async function retryWithTimeout(
    URL: string,
    retryOptions: Partial<RetryOptions> | undefined,
    options: RequestInit
): Promise<Response> {
    const { attempts, codes, delay } = {
        ...defaultRetryOptions,
        ...retryOptions,
    };
    const { timeout = 10000 } = options;
    const controller = new AbortController();
    const timeoutHandle = setTimeout(() => controller.abort(), timeout);
    try {
        const res = await fetch(URL, {
            ...options,
            signal: controller.signal,
        });
        const { status, ok } = res;
        const codeMatch = codes.indexOf(status) > -1;
        if (ok) {
            return res;
        }
        if (attempts === 0 || codeMatch) {
            throw new HTTPResponseError(res);
        }
    } catch (error) {
        if (error.name === 'AbortError') {
            await sleep(delay);
            if (attempts <= 0) {
                throw error;
            }
        } else {
            throw error;
        }
    } finally {
        clearTimeout(timeoutHandle);
    }
    await sleep(delay);
    return retryWithTimeout(
        URL,
        {
            attempts: attempts - 1,
            codes: codes,
            delay: delay,
        },
        options
    );
}

// ====== Request ======
export const METHOD_DELETE = 'DELETE';
export const METHOD_FILE = 'FILE';
export const METHOD_GET = 'GET';
export const METHOD_LONGPOST = 'LONGPOST';
export const METHOD_POST = 'POST';
export const METHOD_PUT = 'PUT';
export interface RequestArgument {
    args: DictStrStr;
    URL: string;
    headers: DictStrStr;
    method: string;
    retry?: Partial<RetryOptions>;
    timeout?: number;
}

export interface BytesRequestArgument extends RequestArgument {
    files?: { [key: string]: any };
}

export interface JSONRequestArgument extends RequestArgument {
    files?: { [key: string]: Buffer };
}

export async function handleError(response: Response) {
    if (response.headers.get('content-type') === 'application/problem+json') {
        const resText = await response.text();
        throw new ServerSideError(`ServerSideError: ${resText}`);
    }
    if (response.status === 403) {
        const resText = await response.text();
        throw new AccessDenied(`Access Denied: ${resText}`);
    }
}
export async function fallibleRawRequestJSON<T>(
    rargs: JSONRequestArgument
): Promise<T> {
    const { method, URL, args, files, retry, ...rest } = rargs;
    const options: RequestInit = {
        method,
        headers: {},
        body: JSON.stringify(args),
        ...(rest as RequestInit),
    };
    if (method !== 'FILE' && files !== undefined) {
        throw new Error(
            `files are only allow for post (got ${method}): ${files}`
        );
    }
    let response: Response | undefined = undefined;

    switch (method) {
        case METHOD_GET: {
            // eslint-disable-next-line @typescript-eslint/no-unused-vars
            const { body, ...init } = options;
            response = await retryWithTimeout(
                getQueryURL(args, URL),
                retry,
                init
            );
            break;
        }
        case METHOD_POST:
        case METHOD_PUT:
        case METHOD_DELETE: {
            response = await retryWithTimeout(URL, retry, options);
            break;
        }
        case METHOD_FILE: {
            const { headers } = options;
            const formData = new FormData();
            if (files) {
                Object.keys(files).map((key) => {
                    formData.append(key, files[key]);
                });
                Object.keys(args).map((key) => {
                    formData.append(key, args[key]);
                });
                response = await retryWithTimeout(URL, retry, {
                    ...options,
                    method: METHOD_POST,
                    body: formData,
                    headers: {
                        ...headers,
                        ...formData.getHeaders(),
                    },
                });
            }
            break;
        }
        default:
            throw new Error(`unknown method ${method}`);
    }
    if (response) {
        handleError(response);
        return await response.json();
    } else {
        throw new Error('no server response');
    }
}

export async function fallibleRawRequestString(
    rargs: RequestArgument
): Promise<Readable> {
    const { method, URL, args, retry, ...rest } = rargs;
    const options: RequestInit = {
        ...rest,
        method,
        body: JSON.stringify(args),
    };
    let response: Response | undefined = undefined;
    switch (method) {
        case METHOD_GET: {
            // eslint-disable-next-line @typescript-eslint/no-unused-vars
            const { body, ...init } = options;
            response = await retryWithTimeout(
                getQueryURL(args, URL),
                retry,
                init
            );
            break;
        }
        case METHOD_POST:
            response = await retryWithTimeout(URL, retry, options);
            break;
        default:
            throw new Error(`unknown method ${method}`);
    }
    if (response) {
        handleError(response);
        try {
            const text = await response.text();
            return Readable.from(text);
        } catch (error) {
            throw new Error('JSON parse error');
        }
    } else {
        throw new Error('no server response');
    }
}

export async function fallibleRawRequestBytes(
    rargs: RequestArgument
): Promise<[Buffer, string]> {
    const { method, URL, args, retry, ...rest } = rargs;
    const options: RequestInit = {
        ...rest,
        method,
        body: JSON.stringify(args),
    };
    let response: Response | undefined = undefined;
    switch (method) {
        case METHOD_GET: {
            // eslint-disable-next-line @typescript-eslint/no-unused-vars
            const { body, ...init } = options;
            response = await retryWithTimeout(
                getQueryURL(args, URL),
                retry,
                init
            );
            break;
        }
        case METHOD_POST:
            response = await retryWithTimeout(URL, retry, options);
            break;
        default:
            throw new Error(`unknown method ${method}`);
    }
    if (response) {
        handleError(response);
        try {
            return [await response.buffer(), response.headers['content-type']];
        } catch (error) {
            throw new Error('JSON parse error');
        }
    } else {
        throw new Error('no server response');
    }
}

export async function rawRequestJSON<T>(
    rargs: JSONRequestArgument
): Promise<T> {
    return await fallibleRawRequestJSON(rargs);
}

export async function rawRequestBytes(
    rargs: BytesRequestArgument
): Promise<[Buffer, string]> {
    return await fallibleRawRequestBytes(rargs);
}

export async function rawRequestString(
    rargs: RequestArgument
): Promise<Readable> {
    return await fallibleRawRequestString(rargs);
}
