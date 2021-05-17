import fetch, { RequestInit, Response } from 'node-fetch';
import AbortController from 'abort-controller';
import { AccessDenied, ServerSideError } from './errors';

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

export async function sleep(time: number) {
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
