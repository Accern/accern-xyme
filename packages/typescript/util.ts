import crypto from 'crypto';
import { promises as fpm } from 'fs';
import { DictStrStr } from './types';
import { KeyError, ServerSideError } from './errors';

const MINUTE = 60.0;
const HOUR = 60.0 * MINUTE;
const DAY = 24.0 * HOUR;
const WEEK = 7.0 * DAY;
const YEAR = 365.0 * DAY;
const FILE_UPLOAD_CHUNK_SIZE = 100 * 1024; // 100kb
const FILE_HASH_CHUNK_SIZE = FILE_UPLOAD_CHUNK_SIZE;

type dict = { [key: string]: any };
export type ByteResponse = Buffer | dict | dict[];

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
    return s === undefined;
}

function isBoolean(s: unknown): s is boolean {
    return typeof s === 'boolean';
}

function isString(s: unknown): s is string {
    return typeof s === 'string';
}

function isDict(s: unknown): s is { [key: string]: any } {
    return s.constructor == Object;
}

function isIterable(obj: any) {
    if (obj == null) {
        return false;
    }
    return typeof obj[Symbol.iterator] === 'function';
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

export function assertDict(value: unknown): { [key: string]: any } {
    if (isDict(value)) {
        return value;
    } else {
        throw new Error(`${value} is not a dictionary`);
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

export async function getReaderHash(
    read: (pos: number, size: number) => Promise<Buffer>
): Promise<[string, number]> {
    const hashObj = crypto.createHash('sha224');
    const chunkSize = FILE_HASH_CHUNK_SIZE;
    let curPos = 0;

    async function readNextChunk(
        chunkSize: number,
        read: (pos: number, size: number) => Promise<Buffer>
    ): Promise<void> {
        const buffer = await read(curPos, chunkSize);
        const nread = buffer.byteLength;
        curPos += nread;
        if (!nread) {
            return;
        }
        hashObj.update(buffer);
        await readNextChunk(chunkSize, read);
    }

    await readNextChunk(chunkSize, read);
    return [hashObj.digest('hex'), curPos];
}

export function getFileUploadChunkSize(): number {
    return FILE_UPLOAD_CHUNK_SIZE;
}

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
    const fileHandle = await fpm.open(fileName, 'r');
    await fileHandle.write(buffer);
}

export function std(arr: number[]): number {
    const mean = arr.reduce((acc, val) => acc + val, 0) / arr.length;
    return Math.sqrt(
        arr.reduce((acc, n) => (n - mean) ** 2) / (arr.length - 1)
    );
}

export function interpretContentType(
    data: Buffer,
    ctype: string
): ByteResponse {
    if (ctype === 'application/json') {
        return JSON.parse(data.toString());
    }
    if (ctype === 'application/problem+json') {
        const res = JSON.parse(data.toString());
        throw new ServerSideError(res['errMessage']);
    }
    if (ctype == 'application/parquet') {
        return data;
    }
    if (ctype == 'application/torch') {
        return data;
    }
    if (ctype == 'application/npz') {
        return data;
    }
    if (ctype == 'application/jsonl') {
        return data;
        // FIXME: use readline
        // return data[
        //     json.load(BytesIO(line))
        //     for line in data
        // ]
    }
    return data;
}

export function mergeContentType(
    datas: ByteResponse[],
    ctype: string
): ByteResponse {
    if (ctype === 'application/json') {
        return datas;
    }
    if (ctype === 'application/parquet') {
        return datas;
    }
    if (ctype === 'application/torch') {
        return datas;
    }
    if (ctype == 'application/npz') {
        return datas;
    }
    if (ctype == 'application/jsonl') {
        if (isIterable(datas)) {
            return datas.flat();
        } else {
            throw new Error(`${datas} is not iterable`);
        }
    }
    return datas;
}
