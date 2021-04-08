/// <reference types="node" />
import * as log4js from 'log4js';
import { FileHandle } from 'fs/promises';
import { DictStrStr } from './types';
export declare function useLogger(catrgory?: string): log4js.Logger;
export declare function safeOptNumber(numOrNull: number | null): [boolean, number];
export declare function getAge(curTime: number, otherTime: number | null): string;
export declare function isUndefined(s: unknown): s is undefined;
export declare function assertString(value: unknown): string;
export declare function assertBoolean(value: unknown): boolean;
export declare function getQueryURL(args: DictStrStr, inURL: string): string;
export declare function getFileHash(fileHandle: FileHandle): Promise<string>;
export declare function getFileUploadChunkSize(): number;
export declare class KeyError extends Error {
}
export declare function forceKey(obj: {
    [key: string]: any;
}, key: string): any;
export declare function openWrite(buffer: Buffer, fileName: string): Promise<void>;
