import * as log4js from 'log4js';
import { open, FileHandle } from 'fs/promises';
import jsSHA from 'jssha';
import { DictStrStr } from './types';

const MINUTE = 60.0;
const HOUR = 60.0 * MINUTE;
const DAY = 24.0 * HOUR;
const WEEK = 7.0 * DAY;
const YEAR = 365.0 * DAY;
const FILE_UPLOAD_CHUNK_SIZE = 100 * 1024; // 100kb
const FILE_HASH_CHUNK_SIZE = FILE_UPLOAD_CHUNK_SIZE;

export function useLogger(catrgory?: string) {
    return log4js.getLogger(catrgory);
}

export function safeOptNumber(numOrNull: number | null): [boolean, number] {
    if (numOrNull === null) {
        return [false, 0.0];
    }
    return [true, numOrNull];
}

export function getAge(curTime: number, otherTime: number | null): string {
    if (otherTime === null) {
        return 'never';
    }

    const diff = curTime - otherTime;
    if (diff < 0.0) {
        return 'soon';
    }
    if (diff < 0.1) {
        return 'now';
    }
    if (diff < 1.0) {
        return '<1s';
    }
    if (diff < MINUTE) {
        return '<1m';
    }
    if (diff < HOUR) {
        return `${Math.floor(diff / MINUTE)}m`;
    }
    if (diff < DAY) {
        return `${Math.floor(diff / HOUR)}h`;
    }
    if (diff < WEEK) {
        return `${Math.floor(diff / DAY)}d`;
    }
    if (diff < YEAR) {
        return `${Math.floor(diff / WEEK)}w`;
    }
    return `${Math.floor(diff / YEAR)}y`;
}

export function isUndefined(s: unknown): s is undefined {
    return typeof s === undefined;
}

function isBoolean(s: unknown): s is boolean {
    return typeof s === 'boolean';
}

function isString(s: unknown): s is string {
    return typeof s === 'string';
}

export function assertString(value: unknown): string {
    if (isString(value)) {
        return value;
    } else {
        throw new Error(`${value} is not string`);
    }
}

export function assertBoolean(value: unknown): boolean {
    if (isBoolean(value)) {
        return value;
    } else {
        throw new Error(`${value} is not boolean`);
    }
}

export function getQueryURL(args: DictStrStr, inURL: string): string {
    const params = new URLSearchParams();
    Object.keys(args).map((key) => {
        params.append(key, args[key]);
    });
    let url = inURL;
    if (params.toString()) {
        url = `${url}?${params.toString()}`;
    }
    return url;
}

export async function getFileHash(fileHandle: FileHandle): Promise<string> {
    const shaObj = new jsSHA('SHA-224', 'BYTES');
    const chunkSize = FILE_HASH_CHUNK_SIZE;
    let curPos = 0;
    async function readNextChunk(
        chunkSize: number,
        fileHandle: FileHandle
    ): Promise<void> {
        const buffer = Buffer.alloc(chunkSize);
        const response = await fileHandle.read(buffer, 0, chunkSize, curPos);
        const nread = response.bytesRead;
        curPos += nread;
        if (nread === 0) {
            return;
        }
        let data: Buffer;
        if (nread < chunkSize) {
            data = buffer.slice(0, nread);
        } else {
            data = buffer;
        }
        shaObj.update(data.toString());
        await readNextChunk(chunkSize, fileHandle);
    }
    await readNextChunk(chunkSize, fileHandle);
    return shaObj.getHash('HEX');
}

export function getFileUploadChunkSize(): number {
    return FILE_UPLOAD_CHUNK_SIZE;
}

export class KeyError extends Error {}

export function forceKey(obj: { [key: string]: any }, key: string): any {
    if (key in obj) {
        return obj[key];
    } else {
        throw new KeyError(
            `unfound key: ${key} in object ${JSON.stringify(obj)}`
        );
    }
}

export async function openWrite(buffer: Buffer, fileName: string) {
    const fileHandle = await open(fileName, 'r');
    await fileHandle.write(buffer);
}
