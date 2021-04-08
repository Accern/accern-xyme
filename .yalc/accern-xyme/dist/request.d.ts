/// <reference types="node" />
/// <reference lib="dom" />
import { RequestInit, Response } from 'node-fetch';
import { DictStrStr } from './types';
import { Readable } from 'stream';
export declare class HTTPResponseError extends Error {
    response: Response;
    constructor(response: Response);
}
export declare type RetryOptions = {
    attempts: number;
    codes: number[];
    delay: number;
};
export declare const defaultRetryOptions: RetryOptions;
export declare function retryWithTimeout(URL: string, retryOptions: Partial<RetryOptions> | undefined, options: RequestInit): Promise<Response>;
export declare const METHOD_DELETE = "DELETE";
export declare const METHOD_FILE = "FILE";
export declare const METHOD_GET = "GET";
export declare const METHOD_LONGPOST = "LONGPOST";
export declare const METHOD_POST = "POST";
export declare const METHOD_PUT = "PUT";
export interface RequestArgument {
    args: DictStrStr;
    URL: string;
    headers: DictStrStr;
    method: string;
    retry?: Partial<RetryOptions>;
    timeout?: number;
}
export interface BytesRequestArgument extends RequestArgument {
    files?: {
        [key: string]: any;
    };
}
export interface JSONRequestArgument extends RequestArgument {
    files?: {
        [key: string]: Buffer;
    };
}
export declare function handleError(response: Response): Promise<void>;
export declare function fallibleRawRequestJSON<T>(rargs: JSONRequestArgument): Promise<T>;
export declare function fallibleRawRequestString(rargs: RequestArgument): Promise<Readable>;
export declare function fallibleRawRequestBytes(rargs: RequestArgument): Promise<Buffer>;
export declare function rawRequestJSON<T>(rargs: JSONRequestArgument): Promise<T>;
export declare function rawRequestBytes(rargs: BytesRequestArgument): Promise<Buffer>;
export declare function rawRequestString(rargs: RequestArgument): Promise<Readable>;
