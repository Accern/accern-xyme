/// <reference lib="dom" />
import { Readable } from 'stream';
import FormData from 'form-data';
import { promises as fpm } from 'fs';
import fetch, { HeadersInit, Response, RequestInit } from 'node-fetch';
import { performance } from 'perf_hooks';
import { isNull } from 'lodash';
import http = require('http');
import https = require('https');
import {
    AllowedCustomImports,
    BlobFilesResponse,
    BlobInit,
    BlobOwner,
    BlobTypeResponse,
    BlobURIResponse,
    CacheStats,
    CopyBlob,
    DagCreate,
    DagInfo,
    DagInit,
    DagList,
    DagPrettyNode,
    DagReload,
    DictStrStr,
    DictStrList,
    DynamicFormat,
    DynamicResults,
    DynamicStatusResponse,
    FlushAllQueuesResponse,
    InCursors,
    InstanceStatus,
    JSONBlobAppendResponse,
    KafkaGroup,
    KafkaMessage,
    KafkaOffsets,
    KafkaThroughput,
    KafkaTopicNames,
    KafkaTopics,
    KnownBlobs,
    MinimalQueueStatsResponse,
    ModelInfo,
    ModelParamsResponse,
    ModelReleaseResponse,
    ModelVersionResponse,
    NamespaceUpdateSettings,
    NodeChunk,
    NodeCustomImports,
    NodeDef,
    NodeDefInfo,
    NodeInfo,
    NodeState,
    NodeStatus,
    NodeTiming,
    NodeTypes,
    NodeUserColumnsResponse,
    PrettyResponse,
    PutNodeBlob,
    QueueMode,
    QueueStatsResponse,
    QueueStatus,
    ReadNode,
    SetNamedSecret,
    SettingsObj,
    TaskStatus,
    Timing,
    TimingResult,
    Timings,
    TritonModelsResponse,
    UUIDResponse,
    UploadFilesResponse,
    VersionResponse,
    WorkerScale,
    NodeTypeResponse,
    DeleteBlobResponse,
    NodeCustomCode,
    URIPrefix,
    UserDagDef,
} from './types';
import {
    handleError,
    METHOD_DELETE,
    METHOD_FILE,
    METHOD_GET,
    METHOD_POST,
    METHOD_PUT,
    RetryOptions,
    sleep,
} from './request';
import {
    assertBoolean,
    assertDict,
    assertString,
    ByteResponse,
    forceKey,
    getAge,
    getFileUploadChunkSize,
    getQueryURL,
    getReaderHash,
    interpretContentType,
    isUndefined,
    mergeContentType,
    openWrite,
    safeOptNumber,
    std,
} from './util';
import { AccessDenied, KeyError, HTTPResponseError } from './errors';
export * from './errors';
export * from './request';
export * from './types';

const API_VERSION = 5;
const MIN_API_VERSION = 4;
const MAX_RETRY = 20;
const RETRY_SLEEP = 5.0;
const EMPTY_BLOB_PREFIX = 'null://';
const PREFIX = 'xyme';
const DEFAULT_NAMESPACE = 'default';
const INPUT_ZIP_EXT = '.zip';
const CUSTOM_NODE_TYPES = [
    'custom_data',
    'custom_json',
    'custom_json_to_data',
    'custom_json_join_data',
];
const NO_RETRY: string[] = []; // [METHOD_POST, METHOD_FILE]
const NO_RETRY_CODE = [403, 404, 500];
const CONSUMER_DAG = 'dag';
type ConsumerType = 'err' | 'err_msg';
const CONSUMER_TYPES: string[] = ['err', 'err_msg'];
const CONSUMER_ERR: ConsumerType = 'err';
const CONSUMER_ERR_MSG: ConsumerType = 'err_msg';
const formCustomCode = (func: string, funcName: string) => `
${func}
result = ${funcName}(*data, **kwargs)
if result is None:
    raise ValueError("${funcName} must return a value")
`;

export interface XYMEConfig {
    url: string;
    token: string;
    namespace?: string;
}

interface XYMERequestArgument {
    addNamespace?: boolean;
    addPrefix?: boolean;
    apiVersion?: number;
    args: { [key: string]: any };
    files?: { [key: string]: Buffer };
    method: string;
    path: string;
    retry?: Partial<RetryOptions>;
}

export class KafkaErrorMessageState {
    msgLookup: Map<string, string>;
    unmatched: string[];

    constructor(config: KafkaErrorMessageState) {
        this.msgLookup = config.msgLookup;
        this.unmatched = config.unmatched;
    }

    public getMsg(input_id: string): string | undefined {
        return this.msgLookup.get(input_id);
    }

    public addMsg(input_id: string, msg: string) {
        this.msgLookup.set(input_id, msg);
    }

    public getUnmatched(): string[] {
        const res = this.unmatched;
        this.unmatched = [];
        return res;
    }

    public addUnmatched(msg: string) {
        this.unmatched.push(msg);
    }
}

export default class XYMEClient {
    httpAgent?: http.Agent;
    httpsAgent?: https.Agent;
    apiVersion?: number;
    apiVersionMinor?: number;
    autoRefresh = true;
    dagCache: WeakMap<{ ['uri']: string }, DagHandle>;
    namespace: string;
    nodeDefs?: { [key: string]: NodeDefInfo };
    token: string;
    url: string;

    constructor(config: XYMEConfig) {
        this.token = config.token;
        this.url = config.url;
        this.namespace = config.namespace || DEFAULT_NAMESPACE;
        this.dagCache = new WeakMap();
        this.httpAgent = new http.Agent({ maxSockets: 10, keepAlive: true });
        this.httpsAgent = new https.Agent({ maxSockets: 10, keepAlive: true });
    }

    public async getAPIVersion(): Promise<number> {
        if (isUndefined(this.apiVersion)) {
            const serverVersions = await this.getServerVersion();

            if (serverVersions.api_version < MIN_API_VERSION) {
                throw new Error(
                    `Legacy version ${serverVersions.api_version}`
                );
            }
            this.apiVersion = serverVersions.api_version;
            const apiVersionMinor = serverVersions.api_version_minor;
            if (isUndefined(apiVersionMinor)) {
                this.apiVersionMinor = 0;
            } else {
                this.apiVersionMinor = apiVersionMinor;
            }
            if (isUndefined(this.apiVersion)) {
                throw new Error('no apiVersion');
            }
        }
        return this.apiVersion;
    }

    public async getAPIVersionMinor(): Promise<number> {
        if (isUndefined(this.apiVersionMinor)) {
            await this.getAPIVersion();
            if (isUndefined(this.apiVersionMinor)) {
                throw new Error('error initializing apiVersionMinor');
            }
        }
        return this.apiVersionMinor;
    }

    public async hasVersion(major: number, minor: number): Promise<boolean> {
        const apiVersion = await this.getAPIVersion();
        const apiVersionMinor = await this.getAPIVersionMinor();
        if (apiVersion > major) {
            return true;
        }
        return apiVersion == major && apiVersionMinor >= minor;
    }

    private setAutoRefresh(autoRefresh: boolean) {
        this.autoRefresh = autoRefresh;
    }

    public isAutoRefresh(): boolean {
        return this.autoRefresh;
    }

    public refresh() {
        this.nodeDefs = undefined;
    }

    private maybeRefresh() {
        if (this.isAutoRefresh()) {
            this.refresh();
        }
    }

    private async getPrefix(
        addPrefix: boolean,
        apiVersion: number | undefined
    ): Promise<string> {
        let prefix = '';
        if (addPrefix) {
            let apiVersionNumber: number;
            if (isUndefined(apiVersion)) {
                apiVersionNumber = await this.getAPIVersion();
            } else {
                apiVersionNumber = apiVersion;
            }
            prefix = `${PREFIX}/v${apiVersionNumber}`;
        }
        return prefix;
    }

    private async rawRequestBytes(
        rargs: XYMERequestArgument
    ): Promise<[Buffer, string]> {
        const {
            addNamespace,
            addPrefix,
            apiVersion,
            args,
            files,
            method,
            path,
        } = rargs;
        let retry = 0;
        // eslint-disable-next-line no-constant-condition
        while (true) {
            try {
                try {
                    return await this.fallibleRawRequestBytes(
                        method,
                        path,
                        args,
                        addPrefix,
                        addNamespace,
                        files,
                        apiVersion
                    );
                } catch (e) {
                    if (
                        e instanceof AccessDenied ||
                        (e instanceof HTTPResponseError &&
                            NO_RETRY_CODE.indexOf(e.response.status) >= 0)
                    ) {
                        retry = MAX_RETRY;
                    }
                    throw e;
                }
            } catch (error) {
                if (retry >= MAX_RETRY) {
                    throw error;
                }
                if (NO_RETRY.indexOf(method) >= 0) {
                    throw error;
                }
                await sleep(RETRY_SLEEP);
            }
            retry += 1;
        }
    }

    private async rawRequestJSON<T>(rargs: XYMERequestArgument): Promise<T> {
        const {
            addNamespace,
            addPrefix,
            apiVersion,
            args,
            files,
            method,
            path,
        } = rargs;
        let retry = 0;
        // eslint-disable-next-line no-constant-condition
        while (true) {
            try {
                try {
                    return await this.fallibleRawRequestJSON(
                        method,
                        path,
                        args,
                        addPrefix,
                        addNamespace,
                        files,
                        apiVersion
                    );
                } catch (e) {
                    if (
                        e instanceof AccessDenied ||
                        (e instanceof HTTPResponseError &&
                            NO_RETRY_CODE.indexOf(e.response.status) >= 0)
                    ) {
                        retry = MAX_RETRY;
                    }
                    throw e;
                }
            } catch (error) {
                if (retry >= MAX_RETRY) {
                    throw error;
                }
                if (NO_RETRY.indexOf(method) >= 0) {
                    throw error;
                }
                await sleep(RETRY_SLEEP);
            }
            retry += 1;
        }
    }

    private async rawRequestString(
        rargs: XYMERequestArgument
    ): Promise<Readable> {
        const {
            addNamespace,
            addPrefix,
            apiVersion,
            args,
            method,
            path,
        } = rargs;
        let retry = 0;
        // eslint-disable-next-line no-constant-condition
        while (true) {
            try {
                try {
                    return await this.fallibleRawRequestString(
                        method,
                        path,
                        args,
                        addPrefix,
                        addNamespace,
                        apiVersion
                    );
                } catch (e) {
                    if (
                        e instanceof AccessDenied ||
                        (e instanceof HTTPResponseError &&
                            NO_RETRY_CODE.indexOf(e.response.status) >= 0)
                    ) {
                        retry = MAX_RETRY;
                    }
                    throw e;
                }
            } catch (error) {
                if (retry >= MAX_RETRY) {
                    throw error;
                }
                if (NO_RETRY.indexOf(method) >= 0) {
                    throw error;
                }
                await sleep(RETRY_SLEEP);
            }
            retry += 1;
        }
    }

    private getAgent(parsedURL: URL): http.Agent | https.Agent {
        if (parsedURL.protocol == 'http:') {
            return this.httpAgent;
        } else {
            return this.httpsAgent;
        }
    }

    private async fallibleRawRequestBytes(
        method: string,
        path: string,
        args: { [key: string]: any },
        addPrefix = true,
        addNamespace = true,
        files?: { [key: string]: Buffer },
        apiVersion?: number
    ): Promise<[Buffer, string]> {
        if (method !== 'FILE' && files !== undefined) {
            throw new Error(
                `files are only allow for post (got ${method}): ${files}`
            );
        }
        const prefix = await this.getPrefix(addPrefix, apiVersion);
        const url = `${this.url}${prefix}${path}`;
        const headers: HeadersInit = {
            authorization: this.token,
        };
        if (addNamespace) {
            args = {
                ...args,
                namespace: this.namespace,
            };
        }
        let options: RequestInit;

        let response: Response | undefined = undefined;

        const parsedURL = new URL(getQueryURL(args, url));
        const agent = this.getAgent(parsedURL);

        switch (method) {
            case METHOD_GET: {
                options = {
                    method,
                    headers,
                    agent,
                };
                response = await fetch(parsedURL, options);
                break;
            }
            case METHOD_POST:
            case METHOD_PUT:
            case METHOD_DELETE: {
                response = await fetch(url, {
                    method: METHOD_POST,
                    headers: {
                        ...headers,
                        'content-type': 'application/json',
                    },
                    body: JSON.stringify(args),
                    agent,
                });
                break;
            }
            case METHOD_FILE: {
                const formData = new FormData();
                if (files) {
                    Object.keys(files).forEach((key) => {
                        const buffCopy = Buffer.from(files[key]);
                        formData.append(key, buffCopy);
                    });
                    Object.keys(args).forEach((key) => {
                        formData.append(key, args[key]);
                    });
                    response = await fetch(url, {
                        method: METHOD_POST,
                        body: formData,
                        headers: {
                            ...headers,
                            ...formData.getHeaders(),
                        },
                        agent,
                    });
                }
                break;
            }
            default:
                throw new Error(`unknown method ${method}`);
        }
        if (response) {
            await handleError(response);
            return [
                await response.buffer(),
                response.headers.get('content-type'),
            ];
        } else {
            throw new Error('no server response');
        }
    }

    private async fallibleRawRequestJSON<T>(
        method: string,
        path: string,
        args: { [key: string]: any },
        addPrefix = true,
        addNamespace = true,
        files?: { [key: string]: Buffer },
        apiVersion?: number
    ): Promise<T> {
        if (method !== 'FILE' && files !== undefined) {
            throw new Error(
                `files are only allow for post (got ${method}): ${files}`
            );
        }
        const prefix = await this.getPrefix(addPrefix, apiVersion);
        const url = `${this.url}${prefix}${path}`;
        const headers: HeadersInit = {
            authorization: this.token,
        };
        if (addNamespace) {
            args = {
                ...args,
                namespace: this.namespace,
            };
        }
        let options: RequestInit;
        let response: Response | undefined = undefined;
        const parsedURL = new URL(getQueryURL(args, url));
        const agent = this.getAgent(parsedURL);

        switch (method) {
            case METHOD_GET: {
                options = {
                    method,
                    headers,
                    agent,
                };
                response = await fetch(parsedURL, options);
                break;
            }
            case METHOD_POST:
            case METHOD_PUT:
            case METHOD_DELETE: {
                options = {
                    method,
                    headers: {
                        ...headers,
                        'content-type': 'application/json',
                    },
                    body: JSON.stringify(args),
                    agent,
                };
                response = await fetch(url, options);
                break;
            }
            case METHOD_FILE: {
                const formData = new FormData();
                if (files) {
                    Object.keys(files).forEach((key) => {
                        const buffCopy = Buffer.from(files[key]);
                        formData.append(key, buffCopy);
                    });
                    Object.keys(args).forEach((key) => {
                        formData.append(key, args[key]);
                    });
                    response = await fetch(url, {
                        method: METHOD_POST,
                        body: formData,
                        headers: {
                            ...headers,
                            ...formData.getHeaders(),
                        },
                        agent,
                    });
                }
                break;
            }
            default:
                throw new Error(`unknown method ${method}`);
        }
        if (response) {
            await handleError(response);
            return await response.json();
        } else {
            throw new Error('no server response');
        }
    }

    private async fallibleRawRequestString(
        method: string,
        path: string,
        args: { [key: string]: any },
        addPrefix = true,
        addNamespace = true,
        apiVersion?: number
    ): Promise<Readable> {
        const prefix = await this.getPrefix(addPrefix, apiVersion);
        const url = `${this.url}${prefix}${path}`;
        let response: Response = undefined;
        const headers: HeadersInit = {
            'Authorization': this.token,
            'content-type': 'application/json',
        };
        if (addNamespace) {
            args = {
                ...args,
                namespace: this.namespace,
            };
        }
        const parsedURL = new URL(getQueryURL(args, url));
        const agent = this.getAgent(parsedURL);
        const options: RequestInit = {
            method,
            headers,
            agent,
        };
        switch (method) {
            case METHOD_GET: {
                response = await fetch(parsedURL, options);
                break;
            }
            case METHOD_POST: {
                response = await fetch(url, options);
                break;
            }
            default:
                throw new Error(`unknown method ${method}`);
        }
        if (response) {
            await handleError(response);
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

    public async requestBytes(
        rargs: XYMERequestArgument
    ): Promise<[Buffer, string]> {
        const {
            addNamespace = true,
            addPrefix = true,
            apiVersion = undefined,
            ...rest
        } = rargs;
        return await this.rawRequestBytes({
            ...rest,
            addNamespace,
            addPrefix,
            apiVersion,
        });
    }

    public async requestJSON<T>(rargs: XYMERequestArgument): Promise<T> {
        const {
            addNamespace = true,
            addPrefix = true,
            apiVersion = undefined,
            ...rest
        } = rargs;
        return await this.rawRequestJSON({
            ...rest,
            addNamespace,
            addPrefix,
            apiVersion,
        });
    }

    public async requestString(rargs: XYMERequestArgument): Promise<Readable> {
        const {
            addNamespace = true,
            addPrefix = true,
            apiVersion = undefined,
            ...rest
        } = rargs;
        return await this.rawRequestString({
            ...rest,
            addNamespace,
            addPrefix,
            apiVersion,
        });
    }

    public async getServerVersion(): Promise<VersionResponse> {
        return await this.requestJSON<VersionResponse>({
            method: METHOD_GET,
            path: `${PREFIX}/v${API_VERSION}/version`,
            addPrefix: false,
            addNamespace: false,
            args: {},
        });
    }

    public async getVersionOverride(): Promise<DictStrList> {
        const serverVersion = await this.getServerVersion();
        const repoTag: DictStrList = {};
        repoTag['versions'] = [
            serverVersion.image_repo,
            serverVersion.image_tag,
        ];
        return repoTag;
    }

    public async getNamespaces(): Promise<string[]> {
        return await this.requestJSON<{ namespaces: string[] }>({
            method: METHOD_GET,
            path: '/namespaces',
            addNamespace: false,
            args: {},
        }).then((response) => response.namespaces);
    }

    public async getDags(): Promise<string[]> {
        const [, dags] = await this.getDagTimes(false);
        return dags.map((dag) => dag.dag);
    }

    public async getDagAges(): Promise<DictStrStr[]> {
        const [curTime, dags] = await this.getDagTimes(true);
        const sorted = dags.sort((a, b) => {
            if (isNull(a.config_error)) {
                return 1;
            }
            if (isNull(b.config_error)) {
                return 0;
            }
            const oldA = safeOptNumber(a.oldest);
            const oldB = safeOptNumber(b.oldest);
            let cmp = +oldA[0] - +oldB[0] || oldA[1] - oldB[1];
            if (cmp !== 0) {
                return cmp;
            }
            const latestA = safeOptNumber(a.latest);
            const latestB = safeOptNumber(b.latest);
            cmp = +latestA[0] - +latestB[0] || latestA[1] - latestB[1];
            if (cmp !== 0) {
                return cmp;
            }
            return +(a.dag >= b.dag);
        });
        const ages: DictStrStr[] = [];
        sorted.forEach((dag) => {
            ages.push({
                configError: dag.config_error,
                created: getAge(curTime, dag.created),
                dag: dag.dag,
                deleted: getAge(curTime, dag.deleted),
                latest: getAge(curTime, dag.latest),
                oldest: getAge(curTime, dag.oldest),
            });
        });
        return ages;
    }

    public async getDagTimes(
        retrieveTimes = true
    ): Promise<[DagList['cur_time'], DagList['dags']]> {
        const response: DagList = await this.requestJSON({
            method: METHOD_GET,
            path: '/dags',
            args: {
                retrieve_times: +retrieveTimes,
            },
        });
        return [response.cur_time, response.dags];
    }

    public async getDag(dagURI: string): Promise<DagHandle> {
        const mDag = this.dagCache.get({ uri: dagURI });
        if (mDag) {
            return mDag;
        }
        const dag = new DagHandle(this, dagURI);
        this.dagCache.set({ uri: dagURI }, dag);
        return dag;
    }

    public getBlobHandle(uri: string, isFull = false): BlobHandle {
        return new BlobHandle(this, uri, isFull);
    }

    public async getNodeDefs(): Promise<NodeTypes['info']> {
        this.maybeRefresh();
        if (this.nodeDefs) {
            return this.nodeDefs;
        }
        const nodeDefs = await this.requestJSON<NodeTypes>({
            method: METHOD_GET,
            path: '/node_types',
            addNamespace: false,
            args: {},
        });
        this.nodeDefs = nodeDefs.info;
        return nodeDefs.info;
    }

    public async createNewBlob(blobType: string): Promise<string> {
        return await this.requestJSON<BlobInit>({
            method: METHOD_POST,
            path: '/blob_init',
            addNamespace: true,
            args: {
                type: blobType,
            },
        }).then((response) => response.blob);
    }

    public async getBlobOwner(blobURI: string): Promise<BlobOwner> {
        return await this.requestJSON<BlobOwner>({
            method: METHOD_GET,
            path: '/blob_owner',
            args: {
                blob: blobURI,
            },
        });
    }

    /**
     * Usage:
     * 1. create a blobURI at you desired location, i.e. s3://bucket/csv/buuid
     * 2. call setBlobOwner('s3://bucer/folder/buuid', null, null, true)
     * 3. BlobOwner will be an external owner
     * {
     *   owner_dag: 'disk://localhost/dag/b00000000000000000000000000000000',
     *   owner_node: 'n00000000000000000000000000000000'
     * }
     * You can use this technique to maintain `external-owned` csv data without
     * breaking the ownership of the blob. These `external-owned` blobs will be
     * read-only to other dags.
     * @param blobURI
     * @param dagId
     * @param nodeId
     * @param externalOwner
     * @returns BlobOwner
     */
    public async setBlobOwner(
        blobURI: string,
        dagId: string = undefined,
        nodeId: string = undefined,
        externalOwner = false
    ): Promise<BlobOwner> {
        return await this.requestJSON<BlobOwner>({
            method: METHOD_PUT,
            path: '/blob_owner',
            args: {
                blob: blobURI,
                owner_dag: dagId,
                owner_node: nodeId,
                external_owner: externalOwner,
            },
        });
    }

    public async createNewDag(
        userName?: string,
        dagName?: string,
        index?: string
    ) {
        return await this.requestJSON<DagInit>({
            method: METHOD_POST,
            path: '/dag_init',
            addNamespace: true,
            args: {
                ...(userName ? { user: userName } : {}),
                ...(dagName ? { name: dagName } : {}),
                ...(index ? { index } : {}),
            },
        }).then((response) => response.dag);
    }

    public async getBlobType(blobURI: string): Promise<BlobTypeResponse> {
        return this.requestJSON<BlobTypeResponse>({
            method: METHOD_GET,
            path: '/blob_type',
            args: {
                blob_uri: blobURI,
            },
        });
    }

    public async getCSVBlob(blobURI: string): Promise<CSVBlobHandle> {
        const blobType = await this.getBlobType(blobURI);
        if (!blobType.is_csv) {
            throw new Error(`blob: ${blobURI} is not csv type`);
        }
        return new CSVBlobHandle(this, blobURI, false);
    }

    public async getTorchBlob(blobURI: string): Promise<TorchBlobHandle> {
        const blobType = await this.getBlobType(blobURI);
        if (!blobType.is_torch) {
            throw new Error(`blob: ${blobURI} is not torch type`);
        }
        return new TorchBlobHandle(this, blobURI, false);
    }

    public async getCustomCodeBlob(
        blobURI: string
    ): Promise<CustomCodeBlobHandle> {
        const blobType = await this.getBlobType(blobURI);
        if (!blobType.is_custom_code) {
            throw new Error(`blob: ${blobURI} is not custom code type`);
        }
        return new CustomCodeBlobHandle(this, blobURI, false);
    }

    public async getJSONBlob(blobURI: string): Promise<JSONBlobHandle> {
        const blobType = await this.getBlobType(blobURI);
        if (!blobType.is_json) {
            throw new Error(`blob: ${blobURI} is not json type`);
        }
        return new JSONBlobHandle(this, blobURI, false);
    }

    public async duplicateDag(
        dagURI: string,
        destURI?: string,
        copyNonownedBlobs = true
    ): Promise<string> {
        return await this.requestJSON<DagCreate>({
            method: METHOD_POST,
            path: '/dag_dup',
            args: {
                dag: dagURI,
                // FIXME: !!!xyme-backend bug!!!
                copy_nonowned_blobs: !copyNonownedBlobs,
                ...(destURI ? { dest: destURI } : {}),
            },
        }).then((response) => response.dag);
    }

    public async duplicateDagNew(
        dagURI: string,
        destURI?: string,
        retainNonownedBlobs = false
    ): Promise<string> {
        return await this.requestJSON<DagCreate>({
            method: METHOD_POST,
            path: '/dag_dup',
            args: {
                dag: dagURI,
                // FIXME: !!!rename in xyme-backend!!!
                copy_nonowned_blobs: retainNonownedBlobs,
                ...(destURI ? { dest: destURI } : {}),
            },
        }).then((response) => response.dag);
    }

    public async setDag(dagURI: string, defs: UserDagDef): Promise<DagHandle> {
        const dagCreate = await this.requestJSON<DagCreate>({
            method: METHOD_POST,
            path: '/dag_create',
            args: {
                dag: dagURI,
                defs,
            },
        });
        const uri = dagCreate.dag;
        const warnings = dagCreate.warnings;
        const numWarnings = warnings.length;
        if (numWarnings > 1) {
            console.info(`
                ${numWarnings} warnings while setting dag ${dagURI}:\n"`);
        } else if (numWarnings === 1) {
            console.info(`Warning while setting dag ${dagURI}:\n`);
        }
        warnings.forEach((warn) => console.info(`${warn}\n`));
        return this.getDag(uri);
    }

    public async setSettings(
        configToken: string,
        settings: SettingsObj
    ): Promise<SettingsObj> {
        return await this.requestJSON<NamespaceUpdateSettings>({
            method: METHOD_POST,
            path: '/settings',
            args: {
                settings,
                config_token: configToken,
            },
        }).then((response) => response.settings);
    }

    public async getSettings(): Promise<SettingsObj> {
        return await this.requestJSON<NamespaceUpdateSettings>({
            method: METHOD_GET,
            path: '/settings',
            args: {},
        }).then((response) => response.settings);
    }

    public async getAllowedCustomImports(): Promise<AllowedCustomImports> {
        return await this.requestJSON<AllowedCustomImports>({
            method: METHOD_GET,
            path: '/allowed_custom_imports',
            addNamespace: false,
            args: {},
        });
    }

    public async checkQueueStats(
        minimal: false,
        dag?: string
    ): Promise<QueueStatsResponse>;

    public async checkQueueStats(
        minimal: true,
        dag?: string
    ): Promise<MinimalQueueStatsResponse>;

    public async checkQueueStats(
        minimal: boolean,
        dag?: string
    ): Promise<MinimalQueueStatsResponse | MinimalQueueStatsResponse> {
        let args: { [key: string]: boolean | string } = {
            minimal,
        };
        if (!isUndefined(dag)) {
            args = {
                ...args,
                dag,
            };
        }
        if (minimal) {
            return await this.requestJSON<MinimalQueueStatsResponse>({
                method: METHOD_GET,
                path: '/queue_stats',
                args,
            });
        } else {
            return this.requestJSON<QueueStatsResponse>({
                method: METHOD_GET,
                path: '/queue_stats',
                args,
            });
        }
    }

    public async getInstanceStatus(
        dagURI?: string,
        nodeId?: string
    ): Promise<{ [key in InstanceStatus]: number }> {
        return await this.requestJSON<{ [key in InstanceStatus]: number }>({
            method: METHOD_GET,
            path: '/instance_status',
            args: {
                dag: dagURI,
                node: nodeId,
            },
        });
    }

    public async getQueueMode(): Promise<string> {
        return await this.requestJSON<QueueMode>({
            method: METHOD_GET,
            path: '/queue_mode',
            args: {},
            addNamespace: false,
        }).then((response) => response.mode);
    }

    public async setQueueMode(mode: string): Promise<string> {
        return await this.requestJSON<QueueMode>({
            method: METHOD_PUT,
            path: '/queue_mode',
            args: {
                mode,
            },
            addNamespace: false,
        }).then((response) => response.mode);
    }

    public async flushAllQueueData(): Promise<void> {
        async function doFlush(that: XYMEClient): Promise<boolean> {
            return await that
                .requestJSON<FlushAllQueuesResponse>({
                    method: METHOD_POST,
                    path: '/flushall_all_queues',
                    args: {},
                    addNamespace: false,
                })
                .then((response) => response.success);
        }
        while (doFlush(this)) {
            // eslint-disable-next-line @typescript-eslint/no-empty-function
            setTimeout(() => {}, 1000);
        }
    }

    public async getCacheStats(): Promise<CacheStats> {
        return await this.requestJSON<CacheStats>({
            method: METHOD_GET,
            path: '/cache_stats',
            args: {},
            addNamespace: false,
        });
    }

    public async resetCache(): Promise<CacheStats> {
        return await this.requestJSON<CacheStats>({
            method: METHOD_POST,
            path: '/cache_reset',
            args: {},
            addNamespace: false,
        });
    }

    public async createKafkaErrorTopic(): Promise<KafkaTopics> {
        return await this.requestJSON<KafkaTopics>({
            method: METHOD_POST,
            path: '/kafka_topics',
            args: {
                num_partitions: 1,
            },
        });
    }

    public async getKafkaErrorTopic(): Promise<string> {
        const res = await this.requestJSON<KafkaTopicNames>({
            method: METHOD_GET,
            path: '/kafka_topic_names',
            args: {},
        }).then((response) => response.error);
        return assertString(res);
    }

    public async getKafkaErrorMessageTopic(): Promise<string> {
        const res = await this.requestJSON<KafkaTopicNames>({
            method: METHOD_GET,
            path: '/kafka_topic_names',
            args: {},
        }).then((response) => response.error_msg);
        return assertString(res);
    }

    public async deleteKafkaErrorTopic(): Promise<KafkaTopics> {
        return await this.requestJSON<KafkaTopics>({
            method: METHOD_POST,
            path: '/kafka_topics',
            args: {
                num_partitions: 0,
            },
        });
    }

    public async readKafkaErrors(
        consumerType: string,
        offset?: string
    ): Promise<string[]> {
        if (CONSUMER_TYPES.includes(consumerType)) {
            throw new Error(
                `consumer_type cannot be  ${consumerType} for reading kafka
                errors. provide consumer type from
                ${CONSUMER_TYPES}`
            );
        }
        return await this.requestJSON<string[]>({
            method: METHOD_GET,
            path: '/kafka_msg',
            args: {
                offset: offset || 'current',
                consumer_type: consumerType,
            },
        });
    }

    /**
     * Provides information as to what the error is and what is the
     * input id and its associated input message.
     * @param inputIdPath: The path of the field to be considered as
     * input_id in the input json.
     * @param errMsgState:
     * This will be populated with the mappings of input_ids to messages.
     * Also stores any unmatched messages in the unmatched list and after
     * filling msg_lookup, check if they have matches. Initially an object
     * of KafkaErrorMessageState can be passed for this argument.
     * @returns
     */
    public async readKafkaFullJsonErrors(
        inputIdPath: string[],
        errMsgState: KafkaErrorMessageState
    ): Promise<[string, string][]> {
        const errs = await this.readKafkaErrors(CONSUMER_ERR);
        const msgs = await this.readKafkaErrors(CONSUMER_ERR_MSG);

        function parseInputIdJson(json_str: string): string | undefined {
            let jres = JSON.parse(json_str);
            Array.from(inputIdPath).forEach((path) => {
                jres = jres[path];
            });
            return jres;
        }

        function parseInputIdText(text: string): string | undefined {
            const ix = text.search('\ninput_id');
            if (ix !== -1) {
                return text.slice(ix + '\ninput_id'.length);
            } else {
                return null;
            }
        }

        Array.from(msgs).forEach((msg) => {
            const inputId = parseInputIdJson(msg);
            if (inputId != null) {
                errMsgState.addMsg(inputId, msg);
            }
        });
        const oldRes: [string, string][] = [];
        Array.from(errMsgState.getUnmatched()).forEach((oldErr) => {
            const inputId = parseInputIdText(oldErr);
            if (inputId != null) {
                const match = errMsgState.getMsg(inputId);
                if (match === null) {
                    errMsgState.addUnmatched(oldErr);
                } else {
                    oldRes.push([oldErr, match]);
                }
            }
        });
        if (oldRes.length) {
            return oldRes;
        }
        const res: [string, string][] = [];
        Array.from(errs).forEach((err) => {
            const inputId = parseInputIdText(err);
            if (inputId != null) {
                const match = errMsgState.getMsg(inputId);
                if (match === null) {
                    errMsgState.addUnmatched(err);
                } else {
                    res.push([err, match]);
                }
            }
        });
        return res;
    }

    public async getNamedSecrets(
        configToken: string | null = null,
        showValues = false
    ): Promise<{ [key: string]: string | null }> {
        if (showValues && !configToken) {
            throw new Error('configToken must be set for showValues');
        }
        return await this.requestJSON<{ [key: string]: string | null }>({
            method: METHOD_GET,
            path: '/named_secrets',
            args: {
                show: +showValues,
                config_token: configToken,
            },
        });
    }

    public async setNamedSecrets(
        configToken: string,
        key: string,
        value: string
    ): Promise<boolean> {
        return await this.requestJSON<SetNamedSecret>({
            method: METHOD_PUT,
            path: '/named_secrets',
            args: {
                key,
                value,
                config_token: configToken,
            },
        }).then((response) => response.replaced);
    }

    public async getErrorLogs(): Promise<string> {
        const stream = await this.requestString({
            method: METHOD_GET,
            path: '/error_logs',
            args: {},
        });
        return await stream.read();
    }

    public async getKnownBlobAges(
        blobType?: string,
        connector?: string
    ): Promise<[string, string][]> {
        const [curTime, blobs] = await this.getKnonwBlobTimes(
            true,
            blobType,
            connector
        );
        const sorted = blobs.sort((a, b) => {
            const oldA = safeOptNumber(a[1]);
            const oldB = safeOptNumber(b[1]);
            const cmp = +oldA[0] - +oldB[0] || oldA[1] - oldB[1];
            if (cmp !== 0) {
                return cmp;
            }
            return +(a[0] >= b[0]);
        });
        const ages: [string, string][] = [];
        sorted.forEach((dag) => {
            ages.push([dag[0], getAge(curTime, dag[1])]);
        });
        return ages;
    }

    public async getKnonwBlobTimes(
        retrieveTimes: boolean,
        blobType?: string,
        connector?: string
    ): Promise<[KnownBlobs['cur_time'], KnownBlobs['blobs']]> {
        const response = await this.requestJSON<KnownBlobs>({
            method: METHOD_GET,
            path: '/known_blobs',
            args: {
                retrieve_times: +retrieveTimes,
                ...(blobType ? { blob_type: blobType } : {}),
                ...(connector ? { connector } : {}),
            },
        });
        return [response.cur_time, response.blobs];
    }

    public async getTritonModels(): Promise<string[]> {
        return await this.requestJSON<TritonModelsResponse>({
            method: METHOD_GET,
            path: '/inference_models',
            args: {},
        }).then((response) => response.models);
    }

    public async getUUID(): Promise<string> {
        return await this.requestJSON<UUIDResponse>({
            method: METHOD_GET,
            path: '/uuid',
            args: {},
        }).then((response) => response.uuid);
    }

    public async deleteBlobs(blobURIs: string[]): Promise<DeleteBlobResponse> {
        return await this.requestJSON<DeleteBlobResponse>({
            method: METHOD_DELETE,
            path: '/blob',
            args: {
                blob_uris: blobURIs,
            },
        });
    }
}

export class DagHandle {
    client: XYMEClient;
    company?: string;
    dynamicError?: string;
    ins?: string[];
    highPriority?: boolean;
    kafkaInputTopic?: string;
    kafkaOutputTopic?: string;
    name?: string;
    nodeLookup: DictStrStr = {};
    nodes: { [key: string]: NodeHandle } = {};
    outs?: [string, string][];
    queueMng?: string;
    stateUri?: string;
    uriPrefix?: URIPrefix;
    uri: string;
    versionOverride?: string;

    constructor(client: XYMEClient, uri: string) {
        this.client = client;
        this.uri = uri;
    }

    public refresh() {
        this.company = undefined;
        this.ins = undefined;
        this.highPriority = undefined;
        this.name = undefined;
        this.kafkaInputTopic = undefined;
        this.kafkaOutputTopic = undefined;
        this.outs = undefined;
        this.queueMng = undefined;
        this.stateUri = undefined;
        this.uriPrefix = undefined;
        this.versionOverride = undefined;
    }

    private maybeRefresh() {
        if (this.client.isAutoRefresh()) {
            this.refresh();
        }
    }

    private async maybeFetch() {
        if (this.name === undefined) {
            await this.fetchInfo();
        }
    }

    private async fetchInfo() {
        const info = await this.getInfo();
        this.name = info.name;
        this.company = info.company;
        this.stateUri = info.state_uri;
        this.versionOverride = info.version_override;
        this.highPriority = info.high_priority;
        this.queueMng = info.queue_mng;
        this.ins = info.ins;
        this.outs = info.outs;
        this.kafkaInputTopic = info.kafka_input_topic;
        this.kafkaOutputTopic = info.kafka_output_topic;
        const oldNodes = this.nodes === undefined ? {} : this.nodes;
        this.nodes = info.nodes.reduce(
            (o, nodeInfo) => ({
                ...o,
                [nodeInfo.id]: NodeHandle.fromNodeInfo(
                    this.client,
                    this,
                    nodeInfo,
                    oldNodes[nodeInfo.id]
                ),
            }),
            {}
        );
        this.nodeLookup = info.nodes.reduce(
            (o, nodeInfo) => ({
                ...o,
                [nodeInfo.name]: nodeInfo.id,
            }),
            {}
        );
    }

    public async getInfo(): Promise<DagInfo> {
        return await this.client.requestJSON<DagInfo>({
            method: METHOD_GET,
            path: '/dag_info',
            addNamespace: true,
            args: {
                dag: this.uri,
            },
        });
    }

    public getURI(): string {
        return this.uri;
    }

    public async getNodes(): Promise<string[]> {
        this.maybeRefresh();
        await this.maybeFetch();
        return Object.keys(this.nodes);
    }

    public async getNode(nodeName: string): Promise<NodeHandle> {
        this.maybeRefresh();
        await this.maybeFetch();
        const nodeId = forceKey(this.nodeLookup, nodeName);
        return forceKey(this.nodes, nodeId);
    }

    public async getName(): Promise<string> {
        this.maybeRefresh();
        await this.maybeFetch();
        return assertString(this.name);
    }

    public async getCompany(): Promise<string> {
        this.maybeRefresh();
        await this.maybeFetch();
        return assertString(this.company);
    }

    public async getStateUri(): Promise<string> {
        this.maybeRefresh();
        await this.maybeFetch();
        return assertString(this.stateUri);
    }

    public async getVersionOverride(): Promise<string> {
        this.maybeRefresh();
        await this.maybeFetch();
        return assertString(this.versionOverride);
    }

    public async getKafkaTopics(): Promise<
        [string | undefined, string | undefined]
    > {
        this.maybeRefresh();
        await this.maybeFetch();
        return [this.kafkaInputTopic, this.kafkaOutputTopic];
    }

    public async getURIPrefix(): Promise<URIPrefix> {
        this.maybeRefresh();
        await this.maybeFetch();
        assertString(this.uriPrefix.connector);
        assertString(this.uriPrefix.address);
        return this.uriPrefix;
    }

    public async getTiming(blacklist?: string[]): Promise<TimingResult> {
        const blist = blacklist ?? [];
        const nodeTiming: { [key: string]: NodeTiming } = {};
        const nodes = await this.getNodes();

        function getFilteredTimes(
            nodeTime: Timing[]
        ): [number, number, Timing[]] {
            let fns: Timing[] = [];
            let nodeTotal = 0.0;
            nodeTime.forEach((value) => {
                if (blist.indexOf(value.name) < 0) {
                    fns = [...fns, value];
                    nodeTotal += value.total;
                }
            });
            if (fns.length <= 0) {
                return [0, 0, fns];
            }
            return [nodeTotal, nodeTotal / fns.length, fns];
        }

        let dagTotal = 0.0;
        nodes.forEach(async (nodeStr) => {
            const node = await this.getNode(nodeStr);
            const nodeTime = await node.getTiming();
            const [nodeTotal, avgTime, fns] = getFilteredTimes(nodeTime);
            const nodeName = await node.getNodeDef().then((res) => res.name);
            nodeTiming[node.getId()] = {
                nodeName,
                nodeTotal,
                nodeAvg: avgTime,
                fns,
            };
            dagTotal += nodeTotal;
        });
        const nodeTimingSorted = Object.entries(nodeTiming).sort(
            ([, a], [, b]) => a['nodeTotal'] - b['nodeTotal']
        );
        return {
            dagTotal,
            nodes: nodeTimingSorted,
        };
    }

    public async isHighPriority(): Promise<boolean> {
        this.maybeRefresh();
        await this.maybeFetch();
        return assertBoolean(this.highPriority);
    }

    public async isQueue(): Promise<boolean> {
        this.maybeRefresh();
        await this.maybeFetch();
        return this.queueMng !== undefined;
    }

    public async getQueueMng(): Promise<string | undefined> {
        this.maybeRefresh();
        await this.maybeFetch();
        return this.queueMng;
    }

    public async getIns(): Promise<string[]> {
        this.maybeRefresh();
        await this.maybeFetch();
        if (isUndefined(this.ins)) {
            throw new Error(`${this.ins} is undefined`);
        } else {
            return this.ins;
        }
    }

    public async getOuts(): Promise<[string, string][]> {
        this.maybeRefresh();
        await this.maybeFetch();
        if (isUndefined(this.outs)) {
            throw new Error(`${this.outs} is undefined`);
        } else {
            return this.outs;
        }
    }

    public async setDag(defs: UserDagDef) {
        await this.client.setDag(this.getURI(), defs);
    }

    public async dynamicModel(
        inputs: any[],
        formatMethod: DynamicFormat = 'simple',
        noCache = false
    ): Promise<any[]> {
        return await this.client
            .requestJSON<DynamicResults>({
                method: METHOD_POST,
                path: '/dynamic_model',
                args: {
                    format: formatMethod,
                    inputs,
                    no_cache: noCache,
                    dag: this.getURI(),
                },
            })
            .then((response) => response.results);
    }

    public async dynamicList(
        inputs: any[],
        fargs: {
            inputKey?: string;
            outputKey?: string;
            splitTh?: number | null;
            formatMethod?: DynamicFormat;
            forceKeys?: boolean;
            noCache?: boolean;
        }
    ): Promise<any[]> {
        const {
            inputKey,
            outputKey,
            splitTh = 1000,
            formatMethod = 'simple',
            forceKeys = false,
            noCache = false,
        } = fargs;
        if (splitTh === null || inputs.length <= splitTh) {
            return await this.client
                .requestJSON<DynamicResults>({
                    method: METHOD_POST,
                    path: '/dynamic_list',
                    args: {
                        force_keys: forceKeys,
                        format: formatMethod,
                        input_key: inputKey,
                        inputs: inputs,
                        no_cache: noCache,
                        output_key: outputKey,
                        dag: this.getURI(),
                    },
                })
                .then((res) => res.results);
        }
        const resArray: any[] = new Array(inputs.length).fill(null);
        const splitNum = splitTh;
        const computeHalf = async (cur: any[], offset: number) => {
            if (cur.length <= splitNum) {
                const curRes = await this.dynamicList(cur, {
                    inputKey,
                    outputKey,
                    splitTh: null,
                    formatMethod,
                    forceKeys,
                    noCache,
                });
                Array.from(Array(curRes.length).keys()).forEach((ix) => {
                    resArray[offset + ix] = curRes[ix];
                });
                return;
            }
            const halfIx = Math.floor(cur.length / 2);
            await Promise.all([
                computeHalf(cur.slice(0, halfIx), offset),
                computeHalf(cur.slice(halfIx), offset + halfIx),
            ]).catch((err) => {
                console.error('Failed to get dynamic list', err);
            });
        };
        await computeHalf(inputs, 0);
        return resArray;
    }

    public async dynamic(inputData: Buffer): Promise<ByteResponse | null> {
        const [res, ctype] = await this.client.requestBytes({
            method: METHOD_FILE,
            path: '/dynamic',
            args: {
                dag: this.getURI(),
            },
            files: {
                file: inputData,
            },
        });
        return interpretContentType(res, ctype);
    }

    public async dynamicObj(inputObj: any): Promise<ByteResponse | null> {
        const buffer = Buffer.from(JSON.stringify(inputObj));
        return this.dynamic(buffer);
    }

    public async dynamicAsync(
        inputData: Buffer[]
    ): Promise<ComputationHandle[]> {
        const range = Array.from(Array(inputData.length).keys());
        const names = range.map((pos) => `file${pos}`);
        const files = range.reduce(
            (o, pos) => ({
                ...o,
                [names[pos]]: inputData[pos],
            }),
            {}
        );
        const res: { [key: string]: string } = await this.client.requestJSON({
            method: METHOD_FILE,
            path: '/dynamic_async',
            args: {
                dag: this.getURI(),
            },
            files,
        });
        return names.map(
            (name) =>
                new ComputationHandle(
                    this,
                    res[name],
                    this.getDynamicErrorMessage,
                    this.setDynamicErrorMessage
                )
        );
    }

    public setDynamicErrorMessage(msg?: string) {
        this.dynamicError = msg;
    }

    public getDynamicErrorMessage(): string | undefined {
        return this.dynamicError;
    }

    public async dynamicAsyncObj(
        inputData: any[]
    ): Promise<ComputationHandle[]> {
        return await this.dynamicAsync(
            inputData.map((inputObj) => Buffer.from(JSON.stringify(inputObj)))
        );
    }

    public async getDynamicResult(
        valueId: string
    ): Promise<ByteResponse | null> {
        try {
            const [res, ctype] = await this.client.requestBytes({
                method: METHOD_GET,
                path: '/dynamic_result',
                args: {
                    dag: this.getURI(),
                    id: valueId,
                },
            });
            return interpretContentType(res, ctype);
        } catch (error) {
            if (!(error instanceof HTTPResponseError)) throw error;
            if (error.response.status === 404) {
                throw new KeyError(`valueId ${valueId} does not exist`);
            }
            throw error;
        }
    }

    public async getDynamicStatus(
        valueIds: ComputationHandle[]
    ): Promise<{ [key: string]: QueueStatus }> {
        const res = await this.client.requestJSON<DynamicStatusResponse>({
            method: METHOD_POST,
            path: '/dynamic_status',
            args: {
                value_ids: valueIds.map((id) => id.getId()),
                dag: this.getURI(),
            },
        });
        const status = res.status;
        let hndMap: { [key: string]: ComputationHandle } = {};
        valueIds.forEach((id) => {
            hndMap = {
                ...hndMap,
                [id.getId()]: id,
            };
        });
        let hndStatus: { [key: string]: QueueStatus } = {};
        Object.keys(status).forEach((key) => {
            hndStatus = {
                ...hndStatus,
                [hndMap[key].valueId]: status[key],
            };
        });
        return hndStatus;
    }

    private async _pretty(
        nodesOnly: boolean,
        allowUnicode: boolean,
        method = 'accern',
        fields: string[] | null = null
    ): Promise<PrettyResponse> {
        return await this.client.requestJSON<PrettyResponse>({
            method: METHOD_GET,
            path: '/pretty',
            args: {
                dag: this.getURI(),
                nodes_only: nodesOnly,
                allow_unicode: allowUnicode,
                method: method,
                fields: fields === null ? undefined : fields.join(','),
            },
        });
    }

    public async pretty(
        nodesOnly = false,
        allowUnicode = true,
        method = 'accern',
        fields: string[] | null = null,
        display = true
    ): Promise<string | undefined> {
        // FIXME: add dot output and allow file like display
        const render = (value: string) => {
            if (display) {
                console.log(value);
                return undefined;
            } else {
                return value;
            }
        };
        const graphStr = await this._pretty(
            nodesOnly,
            allowUnicode,
            method,
            fields
        ).then((res) => res.pretty);
        return render(graphStr);
    }

    public async prettyObj(
        nodesOnly = false,
        allowUnicode = true,
        fields: string[] | null = null
    ): Promise<DagPrettyNode[]> {
        return await this._pretty(
            nodesOnly,
            allowUnicode,
            'accern',
            fields
        ).then((res) => res.nodes);
    }

    public async getDef(full = true): Promise<UserDagDef> {
        return await this.client.requestJSON({
            method: METHOD_GET,
            path: '/dag_def',
            args: {
                dag: this.getURI(),
                full,
            },
        });
    }

    public async setAttr(attr: string, value: any): Promise<void> {
        let dagDef = await this.getDef();
        dagDef = {
            ...dagDef,
            [attr]: value,
        };
        await this.client.setDag(this.getURI(), dagDef);
    }

    public async setName(value: string): Promise<void> {
        await this.setAttr('name', value);
    }

    public async setCompany(value: string): Promise<void> {
        await this.setAttr('company', value);
    }

    public async setStateUri(value: string): Promise<void> {
        await this.setAttr('state_uri', value);
    }

    public async setVersionOverride(value: string): Promise<void> {
        await this.setAttr('version_override', value);
    }

    public async setKafkaInputTopic(value: string | undefined): Promise<void> {
        await this.setAttr('kafka_input_topic', value);
    }

    public async setKafkaOutputTopic(
        value: string | undefined
    ): Promise<void> {
        await this.setAttr('kafka_output_topic', value);
    }

    public async setURIPrefix(value: URIPrefix): Promise<void> {
        await this.setAttr('uri_prefix', value);
    }

    public async setHighPriority(value: string): Promise<void> {
        await this.setAttr('high_priority', value);
    }

    public async setQueueMng(value: string | undefined): Promise<void> {
        await this.setAttr('queue_mng', value);
    }

    public async checkQueueStats(minimal: false): Promise<QueueStatsResponse>;
    public async checkQueueStats(
        minimal: true
    ): Promise<MinimalQueueStatsResponse>;

    public async checkQueueStats(
        minimal: boolean
    ): Promise<MinimalQueueStatsResponse | MinimalQueueStatsResponse> {
        // FIXME: WTF? https://github.com/microsoft/TypeScript/issues/19360
        if (minimal) {
            return await this.client.checkQueueStats(true, this.getURI());
        } else {
            return await this.client.checkQueueStats(false, this.getURI());
        }
    }

    public async scaleWorker(replicas: number): Promise<number> {
        return await this.client
            .requestJSON<WorkerScale>({
                method: METHOD_PUT,
                path: '/worker',
                args: {
                    dag: this.getURI(),
                    replicas,
                    task: undefined,
                },
            })
            .then((res) => res.num_replicas);
    }

    public async reload(timestamp: number | undefined): Promise<number> {
        return await this.client
            .requestJSON<DagReload>({
                method: METHOD_POST,
                path: '/dag_reload',
                args: {
                    dag: this.getURI(),
                    when: timestamp,
                },
            })
            .then((res) => res.when);
    }

    public async getKafkaInputTopic(postfix = ''): Promise<string> {
        const res = await this.client
            .requestJSON<KafkaTopicNames>({
                method: METHOD_GET,
                path: '/kafka_topic_names',
                args: {
                    dag: this.getURI(),
                    postfix: postfix,
                    no_output: true,
                },
            })
            .then((response) => response.input);
        return assertString(res);
    }

    public async getKafkaOutputTopic(): Promise<string> {
        const res = await this.client
            .requestJSON<KafkaTopicNames>({
                method: METHOD_GET,
                path: '/kafka_topic_names',
                args: {
                    dag: this.getURI(),
                },
            })
            .then((response) => response.output);
        return assertString(res);
    }

    public async setKafkaTopicPartitions(fargs: {
        postfix?: string;
        numPartitions?: number;
        largeInputRetention?: boolean;
        noOutput?: boolean;
    }): Promise<KafkaTopics> {
        const {
            postfix = '',
            numPartitions,
            largeInputRetention = false,
            noOutput = false,
        } = fargs;
        return await this.client.requestJSON<KafkaTopics>({
            method: METHOD_POST,
            path: '/kafka_topics',
            args: {
                dag: this.getURI(),
                num_partitions: numPartitions,
                postfix: postfix,
                large_input_retention: largeInputRetention,
                no_output: noOutput,
            },
        });
    }

    public async postKafkaObjs(inputObjs: any[]): Promise<string[]> {
        const bios = inputObjs.reduce(
            (o, inputObj) => [...o, Buffer.from(JSON.stringify(inputObj))],
            []
        );
        return await this.postKafkaMsgs(bios);
    }

    public async postKafkaMsgs(
        inputData: Buffer[],
        postfix = ''
    ): Promise<string[]> {
        const range = Array.from(Array(inputData.length).keys());
        const names: string[] = range.map((ix) => `file${ix}`);

        const files = inputData.reduce(
            (acc, _cV, cIndex) => ({
                ...acc,
                [`file${cIndex}`]: inputData[cIndex],
            }),
            {}
        );
        const msgs = await this.client
            .requestJSON<KafkaMessage>({
                method: METHOD_FILE,
                path: '/kafka_msg',
                args: {
                    dag: this.getURI(),
                    postfix,
                },
                files,
            })
            .then((res) => res.messages);
        return names.map((name) => msgs[name]);
    }

    public async readKafkaOutput(
        offset = 'current',
        maxRows = 100
    ): Promise<ByteResponse | null> {
        const offsetStr = [offset];
        const readSingle = async () => {
            return await this.client.requestBytes({
                method: METHOD_GET,
                path: '/kafka_msg',
                args: {
                    dag: this.getURI(),
                    offset: offsetStr[0],
                    consumer_type: CONSUMER_DAG,
                },
            });
        };
        if (maxRows <= 1) {
            const [res, ctype] = await readSingle();
            return interpretContentType(res, ctype);
        }
        let res: Buffer[] = [];
        let ctype: string | undefined;
        // eslint-disable-next-line no-constant-condition
        while (true) {
            const [val, curContentType] = await readSingle();
            if (val === null) {
                break;
            }
            if (curContentType === null) {
                ctype = curContentType;
            } else if (ctype !== curContentType) {
                throw new Error(
                    `inconsistent return types ${ctype} != ${curContentType}`
                );
            }
            res = [...res, val];
            if (res.length >= maxRows) {
                break;
            }
        }
        if (res.length === 0 || isUndefined(ctype)) {
            return null;
        }
        return mergeContentType(res, ctype);
    }

    public async getKafkaOffsets(
        alive: boolean,
        postfix?: string
    ): Promise<KafkaOffsets> {
        return await this.client.requestJSON({
            method: METHOD_GET,
            path: '/kafka_offsets',
            args: {
                dag: this.getURI(),
                alive: +alive,
                ...(postfix ? { postfix } : {}),
            },
        });
    }

    public async getKafkaThroughput(
        postfix?: string,
        segmentInterval = 120.0,
        segments = 5
    ): Promise<KafkaThroughput> {
        if (segmentInterval <= 0) {
            throw new Error('segmentInterval should be > 0');
        }
        if (segments <= 0) {
            throw new Error('segments should be > 0');
        }
        let offsets = await this.getKafkaOffsets(false, postfix);
        let now = performance.now();
        let measurements: [number, number, number, number, number][] = [
            [
                offsets.input,
                offsets.output,
                offsets.error,
                offsets.error_msg,
                now,
            ],
        ];
        let prev: number;
        const range = Array.from(Array(segments).keys());
        range.forEach(async () => {
            prev = now;
            while (now - prev < segmentInterval) {
                const timeout = Math.max(0.0, segmentInterval - (now - prev));
                // @typescript-eslint/no-empty-function
                // eslint-disable-next-line
                setTimeout(() => {}, timeout);
                now = performance.now();
            }
            offsets = await this.getKafkaOffsets(false);
            measurements = [
                ...measurements,
                [
                    offsets.input,
                    offsets.output,
                    offsets.error,
                    offsets.error_msg,
                    now,
                ],
            ];
        });
        const first = measurements[0];
        const last = measurements[-1];
        const totalInput = last[0] - first[0];
        const totalOutput = last[1] - first[1];
        const errors = last[2] - first[2];
        const errorMsgs = last[3] - first[3];
        const total = last[4] - first[4];
        let inputSegments: number[] = [];
        let outputSegments: number[] = [];
        let curInput = first[0];
        let curOutput = first[1];
        let curTime = first[3];
        measurements.slice(1).forEach((measurement) => {
            const [nextInput, nextOutput, , , nextTime] = measurement;
            const segTime = nextTime - curTime;
            inputSegments = [
                ...inputSegments,
                (nextInput - curInput) / segTime,
            ];
            outputSegments = [
                ...outputSegments,
                (nextOutput - curOutput) / segTime,
            ];
            curInput = nextInput;
            curOutput = nextOutput;
            curTime = nextTime;
        });
        let faster: 'input' | 'output' | 'both' = 'output';
        if (totalInput === totalOutput) {
            faster = 'both';
        } else if (totalInput > totalOutput) {
            faster = 'input';
        }
        return {
            dag: this.getURI(),
            input: {
                throughput: totalInput / total,
                max: Math.max(...inputSegments),
                min: Math.min(...inputSegments),
                stddev: std(inputSegments),
                segments,
                count: totalInput,
                total: total,
            },
            output: {
                throughput: totalOutput / total,
                max: Math.max(...outputSegments),
                min: Math.min(...outputSegments),
                stddev: std(outputSegments),
                segments,
                count: totalOutput,
                total: total,
            },
            faster,
            errors,
            errorMsgs,
        };
    }

    public async getKafkaGroup(): Promise<KafkaGroup> {
        return await this.client.requestJSON({
            method: METHOD_GET,
            path: '/kafka_group',
            args: {
                dag: this.getURI(),
            },
        });
    }

    public async setKafkaGroup(
        groupId: string | undefined,
        reset: string | undefined,
        ...kwargs: any[]
    ): Promise<KafkaGroup> {
        return await this.client.requestJSON<KafkaGroup>({
            method: METHOD_PUT,
            path: '/kafka_group',
            args: {
                dag: this.getURI(),
                groupId,
                reset,
                ...kwargs,
            },
        });
    }

    public async delete(): Promise<DeleteBlobResponse> {
        return await this.client.requestJSON<DeleteBlobResponse>({
            method: METHOD_DELETE,
            path: '/blob',
            args: {
                blob_uris: [this.getURI()],
            },
        });
    }

    public async downloadFullDagZip(
        toPath?: string
    ): Promise<Buffer | undefined> {
        const [res] = await this.client.requestBytes({
            method: METHOD_GET,
            path: '/download_dag_zip',
            args: {
                dag: this.getURI(),
            },
        });
        if (isUndefined(toPath)) {
            return res;
        }
        await openWrite(res, toPath);
        return;
    }
}

export class NodeHandle {
    blobs: { [key: string]: BlobHandle } = {};
    client: XYMEClient;
    configError?: string;
    dag: DagHandle;
    nodeId: string;
    inputs: { [key: string]: [string, string] } = {};
    name: string;
    state?: number;
    versionOverride?: string;
    type: string;
    _isModel?: boolean;

    constructor(
        client: XYMEClient,
        dag: DagHandle,
        nodeId: string,
        name: string,
        kind: string
    ) {
        this.client = client;
        this.dag = dag;
        this.nodeId = nodeId;
        this.name = name;
        this.type = kind;
    }

    public asOwner(): BlobOwner {
        return {
            owner_node: this.getId(),
            owner_dag: this.getDag().getURI(),
        };
    }

    static fromNodeInfo(
        client: XYMEClient,
        dag: DagHandle,
        nodeInfo: NodeInfo,
        prev?: NodeHandle
    ): NodeHandle {
        let node: NodeHandle;
        if (prev === undefined) {
            node = new NodeHandle(
                client,
                dag,
                nodeInfo.id,
                nodeInfo.name,
                nodeInfo.type
            );
        } else {
            if (prev.getDag() !== dag) {
                throw new Error(`dag does${prev.getDag()} != ${dag}`);
            }
            node = prev;
        }
        node.updateInfo(nodeInfo);
        return node;
    }

    private updateInfo(nodeInfo: NodeInfo) {
        if (this.getId() !== nodeInfo.id) {
            throw new Error(`${this.getId()} != ${nodeInfo.id}`);
        }
        this.name = nodeInfo.name;
        this.type = nodeInfo.type;
        this.inputs = nodeInfo.inputs;
        this.state = nodeInfo.state;
        this.configError = nodeInfo.config_error;
        this.versionOverride = nodeInfo.version_override;
        const blobs = nodeInfo.blobs;
        this.blobs = Object.keys(blobs).reduce(
            (o, key) => ({
                ...o,
                [key]: new BlobHandle(this.client, blobs[key], false),
            }),
            {}
        );
    }

    public getDag(): DagHandle {
        return this.dag;
    }

    public getId(): string {
        return this.nodeId;
    }

    public getName(): string {
        return this.name;
    }

    public getType(): string {
        return this.type;
    }

    public getVersionOverride(): string | undefined {
        return this.versionOverride;
    }

    public async getNodeDef(): Promise<NodeDefInfo> {
        const nodeDefs = await this.client.getNodeDefs();
        return nodeDefs[this.getType()];
    }

    public getInputs(): Set<string> {
        return new Set(Object.keys(this.inputs));
    }

    public async getInput(key: string): Promise<[NodeHandle, string]> {
        const [nodeId, outKey] = this.inputs[key];
        return [await this.getDag().getNode(nodeId), outKey];
    }

    public async getStatus(): Promise<TaskStatus> {
        return await this.client
            .requestJSON<NodeStatus>({
                method: METHOD_GET,
                path: '/node_status',
                args: {
                    dag: this.getDag().getURI(),
                    node: this.getId(),
                },
            })
            .then((response) => response.status);
    }

    public hasConfigError(): boolean {
        return this.configError !== undefined;
    }

    public getBlobs(): string[] {
        const blobs = Object.keys(this.blobs);
        blobs.sort();
        return blobs;
    }

    public getBlobHandles(): { [key: string]: BlobHandle } {
        return this.blobs;
    }

    public getBlobHandle(key: string): BlobHandle {
        return this.blobs[key];
    }

    public async setBlobURI(key: string, blobURI: string): Promise<string> {
        return await this.client
            .requestJSON<PutNodeBlob>({
                method: METHOD_PUT,
                path: '/node_blob',
                args: {
                    dag: this.getDag().getURI(),
                    node: this.getId(),
                    blob_key: key,
                    blob_uri: blobURI,
                },
            })
            .then((response) => response.new_uri);
    }

    public async getInCursorStates(): Promise<{ [key: string]: number }> {
        return await this.client
            .requestJSON<InCursors>({
                method: METHOD_GET,
                path: '/node_in_cursors',
                args: {
                    dag: this.getDag().getURI(),
                    node: this.getId(),
                },
            })
            .then((response) => response.cursors);
    }

    public async getHighestChunk(): Promise<number> {
        return await this.client
            .requestJSON<NodeChunk>({
                method: METHOD_GET,
                path: '/node_chunk',
                args: {
                    dag: this.getDag().getURI(),
                    node: this.getId(),
                },
            })
            .then((response) => response.chunk);
    }

    public async getShortStatus(allowUnicode = true): Promise<string> {
        const status_map: { [key in TaskStatus]: string } = {
            blocked: 'B',
            waiting: 'W',
            running: allowUnicode ? '→' : 'R',
            complete: allowUnicode ? '✓' : 'C',
            eos: 'X',
            paused: 'P',
            error: '!',
            unknown: '?',
            virtual: allowUnicode ? '∴' : 'V',
            queue: '=',
        };
        return status_map[await this.getStatus()];
    }

    public async getLogs(): Promise<string> {
        const textStream = await this.client.requestString({
            method: METHOD_GET,
            path: '/node_logs',
            args: {
                dag: this.getDag().getURI(),
                node: this.getId(),
            },
        });
        const logs = textStream.read();
        return logs;
    }

    public async getTiming(): Promise<Timing[]> {
        return await this.client
            .requestJSON<Timings>({
                method: METHOD_GET,
                path: '/node_perf',
                args: {
                    dag: this.getDag().getURI(),
                    node: this.getId(),
                },
            })
            .then((response) => response.times);
    }

    public async readBlob(
        key: string,
        chunk: number | undefined,
        forceRefresh?: boolean
    ): Promise<BlobHandle> {
        const uri = await this.client
            .requestJSON<ReadNode>({
                method: METHOD_POST,
                path: '/read_node',
                args: {
                    dag: this.getDag().getURI(),
                    node: this.getId(),
                    key,
                    chunk,
                    is_blocking: true,
                    force_refresh: forceRefresh || false,
                },
            })
            .then((response) => response.result_uri);
        if (uri === undefined) {
            throw new Error('uri is undefined');
        }
        return new BlobHandle(this.client, uri, true);
    }

    public async readBlobNonblocking(
        key: string,
        chunk: number | undefined,
        forceRefresh?: boolean
    ): Promise<string> {
        const redis_key = await this.client
            .requestJSON<ReadNode>({
                method: METHOD_POST,
                path: '/read_node',
                args: {
                    dag: this.getDag().getURI(),
                    node: this.getId(),
                    key,
                    chunk,
                    is_blocking: false,
                    force_refresh: forceRefresh || false,
                },
            })
            .then((response) => response.redis_key);
        return redis_key;
    }

    public async getIndexCol(): Promise<string> {
        return (await this.client.hasVersion(4, 1)) ? '_index' : 'index';
    }

    public async getRowIdCol(): Promise<string> {
        return (await this.client.hasVersion(4, 1)) ? '_row_id' : 'row_id';
    }

    public async read(
        key: string,
        chunk: number | null,
        forceRefresh?: boolean
    ): Promise<ByteResponse | null> {
        const blob = await this.readBlob(key, chunk, forceRefresh || false);
        return await blob.getContent();
    }

    /**
     * Read and combine all output chunks.
     */
    public async readAll(
        key: string,
        forceRefresh = false
    ): Promise<ByteResponse | null> {
        await this.read(key, null, forceRefresh);
        let res: ByteResponse[] = [];
        let ctype: string | undefined;
        // eslint-disable-next-line no-constant-condition
        while (true) {
            const blob = await this.readBlob(key, res.length, false);
            if (!blob) break;

            const cur = await blob.getContent();
            if (!cur) break;
            const curCtype = blob.getContentType();
            if (curCtype === null) {
                ctype = curCtype;
            } else if (ctype !== curCtype) {
                throw new Error(
                    `inconsistent return types ${ctype} != ${curCtype}`
                );
            }
            res = [...res, cur];
        }
        if (res.length === 0 || isUndefined(ctype)) {
            return null;
        }
        return mergeContentType(res, ctype);
    }

    public async clear(): Promise<NodeState> {
        return this.client.requestJSON<NodeState>({
            method: METHOD_PUT,
            path: '/node_state',
            args: {
                dag: this.getDag().getURI(),
                node: this.getId(),
                action: 'reset',
            },
        });
    }

    public async requeue(): Promise<NodeState> {
        return this.client.requestJSON<NodeState>({
            method: METHOD_PUT,
            path: '/requeue',
            args: {
                dag: this.getDag().getURI(),
                node: this.getId(),
                action: 'requeue',
            },
        });
    }

    public async getBlobURI(
        blobKey: string,
        blobType: string
    ): Promise<[string, BlobOwner]> {
        const res = await this.client.requestJSON<BlobURIResponse>({
            method: METHOD_GET,
            path: '/blob_uri',
            args: {
                dag: this.getDag().getURI(),
                node: this.getId(),
                key: blobKey,
                type: blobType,
            },
        });
        return [res.uri, res.owner];
    }

    public async getCSVBlob(key = 'orig'): Promise<CSVBlobHandle> {
        const [uri, owner] = await this.getBlobURI(key, 'csv');
        const blob = new CSVBlobHandle(this.client, uri, false);
        blob.setLocalOwner(owner);
        return blob;
    }

    public async getTorchBlob(key = 'orig'): Promise<TorchBlobHandle> {
        const [uri, owner] = await this.getBlobURI(key, 'torch');
        const blob = new TorchBlobHandle(this.client, uri, false);
        blob.setLocalOwner(owner);
        return blob;
    }

    public async getJSONBlob(key = 'jsons_in'): Promise<JSONBlobHandle> {
        const [uri, owner] = await this.getBlobURI(key, 'json');
        const blob = new JSONBlobHandle(this.client, uri, false);
        blob.setLocalOwner(owner);
        return blob;
    }

    public async getCustomCodeBlob(
        key = 'custom_code'
    ): Promise<CustomCodeBlobHandle> {
        const [uri, owner] = await this.getBlobURI(key, 'custom_code');
        const blob = new CustomCodeBlobHandle(this.client, uri, false);
        blob.setLocalOwner(owner);
        return blob;
    }

    public checkCustomCodeNode() {
        if (CUSTOM_NODE_TYPES.indexOf(this.getType()) < 0) {
            throw new Error(`${this} is not a custom code node`);
        }
    }

    public async getUserColumn(key: string): Promise<NodeUserColumnsResponse> {
        return await this.client.requestJSON<NodeUserColumnsResponse>({
            method: METHOD_GET,
            path: '/user_columns',
            args: {
                dag: this.getDag().getURI(),
                node: this.getId(),
                key,
            },
        });
    }

    public async getDef(): Promise<NodeDef> {
        return await this.client.requestJSON<NodeDef>({
            method: METHOD_GET,
            path: '/node_def',
            args: {
                dag: this.getDag().getURI(),
                node: this.getId(),
            },
        });
    }

    // ModelLike Nodes only

    public async isModel(): Promise<boolean> {
        if (this._isModel === null) {
            this._isModel = await this.client
                .requestJSON<NodeTypeResponse>({
                    method: METHOD_GET,
                    path: '/node_type',
                    args: {
                        dag: this.getDag().getURI(),
                        node: this.getId(),
                    },
                })
                .then((response) => response.is_model);
        }
        return this._isModel;
    }

    public async ensureIsModel() {
        const res = await this.isModel();
        if (!res) {
            throw new Error(`${this} is not a model node.`);
        }
    }

    public async setupModel(obj: { [key: string]: any }): Promise<ModelInfo> {
        await this.ensureIsModel();
        return await this.client.requestJSON<ModelInfo>({
            method: METHOD_PUT,
            path: '/model_setup',
            args: {
                dag: this.getDag().getURI(),
                node: this.getId(),
                config: obj,
            },
        });
    }

    public async getModelParams(): Promise<ModelParamsResponse> {
        await this.ensureIsModel();
        return await this.client.requestJSON<ModelParamsResponse>({
            method: METHOD_GET,
            path: '/model_params',
            args: {
                dag: this.getDag().getURI(),
                node: this.getId(),
            },
        });
    }
}

export class BlobHandle {
    client: XYMEClient;
    uri: string;
    isFull: boolean;
    ctype?: string;
    tmpURI?: string;
    owner?: BlobOwner;
    info?: Promise<{ [key: string]: any }>;

    constructor(client: XYMEClient, uri: string, isFull: boolean) {
        this.client = client;
        this.uri = uri;
        this.isFull = isFull;
        this.ctype = null;
        this.tmpURI = null;
        this.owner = null;
    }

    public isEmpty(): boolean {
        return this.uri.startsWith(EMPTY_BLOB_PREFIX);
    }

    public getURI(): string {
        return this.uri;
    }

    public getPath(path: string[]): BlobHandle {
        this.ensureNotFull();
        return new BlobHandle(
            this.client,
            `${this.uri}/${path.join('/')}`,
            true
        );
    }

    public getContentType(): string | undefined {
        return this.ctype;
    }

    public clearInfoCache() {
        this.info = null;
    }

    public async getInfo(): Promise<{ [key: string]: any }> {
        this.ensureNotFull();
        if (!this.info) {
            this.info = this.getPath(['info.json'])
                .getContent()
                .then(assertDict);
        }
        return this.info;
    }

    public async getContent(): Promise<ByteResponse | null> {
        this.ensureIsFull();
        if (this.isEmpty()) {
            return null;
        }

        const totalTime = 60.0;
        let sleepTime = 0.1;
        const sleepMul = 1.1;
        const sleepMax = 5.0;
        const startTime = performance.now();
        while (performance.now() - startTime <= totalTime) {
            try {
                const retryOptions: Partial<RetryOptions> = {
                    attempts: 1,
                    delay: sleepTime,
                };
                const [res, ctype] = await this.client.requestBytes({
                    method: METHOD_POST,
                    path: '/uri',
                    args: {
                        uri: this.getURI(),
                    },
                    retry: retryOptions,
                });
                return interpretContentType(res, ctype);
            } catch (error) {
                if (!(error instanceof HTTPResponseError)) throw error;
                if (error.response.status === 404) {
                    throw error;
                }
                sleepTime = Math.min(sleepTime * sleepMul, sleepMax);
            }
        }
        throw new Error('timeout reading content');
    }

    private asStr(): string {
        return `${this.uri}`;
    }

    private ensureIsFull() {
        if (!this.isFull) throw new Error(`URI must be full: ${this.uri}`);
    }

    private ensureNotFull() {
        if (this.isFull) throw new Error(`URI must not be full: ${this.uri}`);
    }

    public async listFiles(): Promise<BlobHandle[]> {
        this.ensureNotFull();
        const response = await this.client.requestJSON<BlobFilesResponse>({
            method: METHOD_GET,
            path: '/blob_files',
            args: {
                blob: this.uri,
            },
        });
        return response.files.map((blob) => {
            return new BlobHandle(this.client, blob, true);
        });
    }

    public async setOwner(owner: NodeHandle): Promise<BlobOwner> {
        this.ensureNotFull();
        return this.client.requestJSON<BlobOwner>({
            method: METHOD_PUT,
            path: '/blob_owner',
            args: {
                blob: this.uri,
                owner_dag: owner.getDag().getURI(),
                owner_node: owner.getId(),
            },
        });
    }

    public async getOwner(): Promise<BlobOwner> {
        if (!this.owner) {
            this.owner = await this.client.getBlobOwner(this.uri);
        }
        return this.owner;
    }

    public setLocalOwner(owner: BlobOwner) {
        this.owner = owner;
    }

    public async getOwnerDag(): Promise<string> {
        const owner = await this.getOwner();
        return owner.owner_dag;
    }

    public async getOwnerNode(): Promise<string> {
        const owner = await this.getOwner();
        return owner.owner_node;
    }

    /**
     * User can pass `externalOwner: true` to set the blob at toURI as
     * external-owned blob.
     */

    public async setModelThreshold(
        threshold: number,
        pos_label: string
    ): Promise<ModelInfo> {
        return await this.client.requestJSON<ModelInfo>({
            method: METHOD_PUT,
            path: '/threshold',
            args: {
                blob: this.getURI(),
                threshold: threshold,
                pos_label: pos_label,
            },
        });
    }

    public async delModelThreshold(): Promise<ModelInfo> {
        return await this.client.requestJSON<ModelInfo>({
            method: METHOD_DELETE,
            path: '/threshold',
            args: {
                blob: this.getURI(),
            },
        });
    }

    public async getModelInfo(): Promise<ModelInfo> {
        return await this.client.requestJSON<ModelInfo>({
            method: METHOD_GET,
            path: '/model_info',
            args: {
                blob: this.getURI(),
            },
        });
    }

    public async copyTo(
        toURI: string,
        newOwner: NodeHandle | undefined,
        externalOwner = false
    ): Promise<BlobHandle> {
        this.ensureNotFull();
        const ownerDag =
            newOwner !== undefined ? newOwner.getDag().getURI() : undefined;
        const ownerNode =
            newOwner !== undefined ? newOwner.getId() : undefined;
        const response = await this.client.requestJSON<CopyBlob>({
            method: METHOD_POST,
            path: '/copy_blob',
            args: {
                from_uri: this.uri,
                owner_dag: ownerDag,
                owner_node: ownerNode,
                externalOwner,
                to_uri: toURI,
            },
        });
        return new BlobHandle(this.client, response.new_uri, false);
    }

    public async downloadZip(toPath?: string): Promise<Buffer | undefined> {
        if (this.isFull) {
            throw new Error(`URI must not be full: ${this.getURI()}`);
        }
        const [res] = await this.client.requestBytes({
            method: METHOD_GET,
            path: '/download_zip',
            args: {
                blob: this.getURI(),
            },
        });
        if (isUndefined(toPath)) {
            return res;
        }
        await openWrite(res, toPath);
        return;
    }

    public async performUploadAction(
        action: string,
        additional: { [key: string]: string | number },
        fobj: Buffer | null
    ): Promise<UploadFilesResponse> {
        const args: { [key: string]: string | number } = {
            ...additional,
            action,
        };
        let method: string;
        let files: { [key: string]: Buffer } | undefined = undefined;
        if (fobj !== null) {
            method = METHOD_FILE;
            files = {
                file: fobj,
            };
        } else {
            method = METHOD_POST;
            files = undefined;
        }
        if (action == 'clear') {
            this.tmpURI = undefined;
        }
        return this.client.requestJSON<UploadFilesResponse>({
            method,
            path: '/upload_file',
            args,
            files,
        });
    }

    public async startUpload(
        size: number,
        hashStr: string,
        ext: string
    ): Promise<string> {
        const res = await this.performUploadAction(
            'start',
            {
                target: this.getURI(),
                hash: hashStr,
                size,
                ext,
            },
            null
        );
        const uri = res.uri;
        if (isUndefined(uri)) {
            throw new Error('uri undefined');
        }
        return uri;
    }

    private async legacyAppendUpload(
        uri: string,
        fobj: Buffer
    ): Promise<number> {
        const res = await this.performUploadAction('append', { uri }, fobj);
        return res.pos;
    }

    private async appendUpload(
        uri: string,
        offset: number,
        fobj: Buffer
    ): Promise<number> {
        const res = await this.performUploadAction(
            'append',
            { uri, offset },
            fobj
        );
        return res.pos;
    }

    public async finishUploadZip(): Promise<string[]> {
        const uri = this.tmpURI;
        if (isUndefined(uri)) {
            throw new Error('uri undefined');
        }
        return await this.client
            .requestJSON<UploadFilesResponse>({
                method: METHOD_POST,
                path: '/finish_zip',
                args: {
                    uri,
                },
            })
            .then((res) => res.files);
    }

    public async clearUpload() {
        const uri = this.tmpURI;
        if (!isUndefined(uri)) {
            await this.performUploadAction('clear', { uri }, null);
        }
    }

    /**
     * This is the helper method being used by uploadFile
     * and uploadFileUsingContent
     * @param buffer: the buffer chunk being uploaded
     * @param nread: number of bytes from the in the buffer
     * @param offset: start byte position of part
     * @param blobHandle: the parent this being passed here
     * @returns
     */
    private async updateBuffer(
        read: (pos: number, size: number) => Promise<Buffer>,
        offset: number,
        blobHandle: BlobHandle
    ) {
        const buffer = await read(offset, getFileUploadChunkSize());
        const newSize = await blobHandle.appendUpload(
            this.tmpURI,
            offset,
            buffer
        );
        if (newSize !== buffer.length) {
            throw new Error(`
                incomplete chunk upload n:${newSize} b: ${buffer.length}
            `);
        }
        return newSize;
    }

    private async uploadReader(
        read: (pos: number, size: number) => Promise<Buffer>,
        ext: string,
        progressBar?: WritableStream,
        method?: string
    ): Promise<void> {
        if (this.client.apiVersion < 5) {
            await this.legacyUploadReader(read, ext, progressBar, method);
            return;
        }
        if (progressBar !== undefined) {
            const methodStr = method !== undefined ? ` ${method}` : '';
            progressBar.getWriter().write(`Uploading${methodStr}:\n`);
        }
        const [hash, totalSize] = await getReaderHash(read);
        const tmpURI = await this.startUpload(totalSize, hash, ext);
        this.tmpURI = tmpURI;
        const uploadChunkSize = getFileUploadChunkSize();
        const totalChunks = Math.ceil(totalSize / uploadChunkSize);
        const begins: number[] = [];
        Array.from(Array(totalChunks).keys()).forEach((chunk) => {
            begins.push(chunk * uploadChunkSize);
        });
        await Promise.all(
            begins.map((offset) => {
                return this.updateBuffer(read, offset, this);
            })
        ).catch((err) => {
            this.clearUpload();
            throw err;
        });
    }

    private async legacyUpdateBuffer(
        curSize: number,
        buffer: Buffer,
        nread: number,
        chunk: number,
        blobHandle: BlobHandle
    ) {
        let data: Buffer;
        if (nread < chunk) {
            data = buffer.slice(0, nread);
        } else {
            data = buffer;
        }
        const newSize = await blobHandle.legacyAppendUpload(this.tmpURI, data);
        if (newSize - curSize !== data.length) {
            throw new Error(`
                incomplete chunk upload n:${newSize} o:${curSize}
                b: ${data.length}
            `);
        }
        return newSize;
    }

    private async legacyUploadReader(
        read: (pos: number, size: number) => Promise<Buffer>,
        ext: string,
        progressBar?: WritableStream,
        method?: string
    ): Promise<void> {
        if (progressBar !== undefined) {
            const methodStr = method !== undefined ? ` ${method}` : '';
            progressBar.getWriter().write(`Uploading${methodStr}:\n`);
        }

        const [hash, totalSize] = await getReaderHash(read);
        const tmpURI = await this.startUpload(totalSize, hash, ext);
        this.tmpURI = tmpURI;
        let curPos = 0;
        let curSize = 0;

        async function uploadNextChunk(
            blobHandle: BlobHandle,
            chunk: number,
            read: (pos: number, size: number) => Promise<Buffer>
        ): Promise<void> {
            const buffer = await read(curPos, chunk);
            const nread = buffer.byteLength;
            if (!nread) {
                return;
            }
            curPos += nread;
            const newSize = await blobHandle.legacyUpdateBuffer(
                curSize,
                buffer,
                nread,
                chunk,
                blobHandle
            );
            curSize = newSize;
            await uploadNextChunk(blobHandle, chunk, read);
        }

        await uploadNextChunk(this, getFileUploadChunkSize(), read).catch(
            (error) => {
                this.clearUpload();
                throw error;
            }
        );
    }

    public async uploadFile(
        fileContent: fpm.FileHandle,
        ext: string,
        progressBar?: WritableStream
    ): Promise<void> {
        async function readFile(pos: number, size: number): Promise<Buffer> {
            const buffer = Buffer.alloc(size);
            const response = await fileContent.read(buffer, 0, size, pos);
            const nread = response.bytesRead;
            return buffer.slice(0, nread);
        }
        return await this.uploadReader(readFile, ext, progressBar, 'File');
    }

    /**
     * This prototype method allows you to upload the file using content Buffer
     * @param contentBuffer: file content as Buffer object
     * @param ext: The file extension (e.g .csv)
     * @param progressBar: stream where we show the upload progress
     */
    public async uploadFileUsingContent(
        contentBuffer: Buffer,
        ext: string,
        progressBar?: WritableStream
    ): Promise<void> {
        async function readBuffer(pos: number, size: number): Promise<Buffer> {
            return contentBuffer.slice(pos, pos + size);
        }

        return await this.uploadReader(readBuffer, ext, progressBar, 'Buffer');
    }

    public async uploadZip(
        source: string | fpm.FileHandle
    ): Promise<BlobHandle[]> {
        let files: string[] = [];
        try {
            if (typeof source === 'string') {
                const fileHandle = await fpm.open(source, 'r');
                this.uploadFile(fileHandle, 'zip');
            } else {
                this.uploadFile(source, 'zip');
            }
            files = await this.finishUploadZip();
        } catch (error) {
            this.clearUpload();
        }
        return files.map(
            (blobURI) => new BlobHandle(this.client, blobURI, true)
        );
    }

    public async convertModel(
        version: number = null,
        reload = true
    ): Promise<ModelReleaseResponse> {
        return await this.client.requestJSON<ModelReleaseResponse>({
            method: METHOD_POST,
            path: '/convert_model',
            args: {
                blob: this.getURI(),
                version,
                reload,
            },
        });
    }

    public async getModelRelease(): Promise<ModelReleaseResponse> {
        return await this.client.requestJSON<ModelReleaseResponse>({
            method: METHOD_GET,
            path: '/model_release',
            args: {
                blob: this.getURI(),
            },
        });
    }

    public async getModelVersion(): Promise<ModelVersionResponse> {
        return await this.client.requestJSON<ModelVersionResponse>({
            method: METHOD_GET,
            path: '/model_version',
            args: {
                model_uri: this.getURI(),
            },
        });
    }

    public async copyModelVersion(
        readVersion: number,
        writeVersion: number,
        overwrite: boolean
    ): Promise<ModelVersionResponse> {
        return await this.client.requestJSON<ModelVersionResponse>({
            method: METHOD_PUT,
            path: '/model_version',
            args: {
                model_uri: this.getURI(),
                read_version: readVersion,
                write_version: writeVersion,
                overwrite: overwrite,
            },
        });
    }

    public async deleteModelVersion(
        version: number
    ): Promise<ModelVersionResponse> {
        return await this.client.requestJSON<ModelVersionResponse>({
            method: METHOD_PUT,
            path: '/model_version',
            args: {
                model_uri: this.getURI(),
                read_version: null,
                write_version: version,
                overwrite: true,
            },
        });
    }

    public async delete(): Promise<DeleteBlobResponse> {
        return await this.client.requestJSON<DeleteBlobResponse>({
            method: METHOD_DELETE,
            path: '/blob',
            args: {
                blob_uris: [this.getURI()],
            },
        });
    }
}

export class CSVBlobHandle extends BlobHandle {
    public async addFromFile(
        fileName: string,
        progressBar: WritableStream | undefined = undefined,
        requeueOnFinish: NodeHandle | undefined = undefined
    ) {
        let fname = fileName;
        if (fileName.endsWith(INPUT_ZIP_EXT)) {
            fname = fileName.slice(0, -INPUT_ZIP_EXT.length);
        }
        const extPos = fname.indexOf('.');
        let ext: string;
        if (extPos > 0) {
            ext = fileName.slice(extPos + 1);
        } else {
            throw new Error('could not determine extension');
        }
        const fileHandle = await fpm.open(fileName, 'r');

        try {
            await this.uploadFile(fileHandle, ext, progressBar);
            return await this.finishCSVUpload(fileName);
        } finally {
            if (!isUndefined(requeueOnFinish)) {
                requeueOnFinish.requeue();
            }
            await fileHandle.close();
            await this.clearUpload();
        }
    }

    public async addFromContent(
        fileName: string,
        content: Buffer,
        progressBar: WritableStream | undefined = undefined,
        requeueOnFinish: NodeHandle | undefined = undefined
    ) {
        let fname = fileName;
        if (fileName.endsWith(INPUT_ZIP_EXT)) {
            fname = fileName.slice(0, -INPUT_ZIP_EXT.length);
        }
        const extPos = fname.indexOf('.');
        let ext: string;
        if (extPos > 0) {
            ext = fileName.slice(extPos + 1);
        } else {
            throw new Error('could not determine extension');
        }

        try {
            await this.uploadFileUsingContent(content, ext, progressBar);
            return await this.finishCSVUpload(fileName);
        } finally {
            if (!isUndefined(requeueOnFinish)) {
                requeueOnFinish.requeue();
            }
            await this.clearUpload();
        }
    }

    public async finishCSVUpload(
        fileName?: string
    ): Promise<UploadFilesResponse> {
        const tmpURI = this.tmpURI;
        if (isUndefined(tmpURI)) {
            throw new Error('uri undefined');
        }
        return await this.client.requestJSON({
            method: METHOD_POST,
            path: '/finish_csv',
            args: {
                tmp_uri: tmpURI,
                csv_uri: this.getURI(),
                owner_dag: await this.getOwnerDag(),
                owner_node: await this.getOwnerNode(),
                filename: fileName,
            },
        });
    }
}

// *** CSVBlobHandle ***

export class TorchBlobHandle extends BlobHandle {
    public async addFromFile(
        fileName: string,
        progressBar: WritableStream | undefined = undefined
    ) {
        let fname = fileName;
        if (fileName.endsWith(INPUT_ZIP_EXT)) {
            fname = fileName.slice(0, -INPUT_ZIP_EXT.length);
        }
        const extPos = fname.indexOf('.');
        let ext: string;
        if (extPos > 0) {
            ext = fileName.slice(extPos + 1);
        } else {
            throw new Error('could not determine extension');
        }
        const fileHandle = await fpm.open(fileName, 'r');

        try {
            await this.uploadFile(fileHandle, ext, progressBar);
            return await this.finishTorchUpload(fileName);
        } finally {
            await fileHandle.close();
            await this.clearUpload();
        }
    }

    public async finishTorchUpload(
        fileName?: string
    ): Promise<UploadFilesResponse> {
        const tmpURI = this.tmpURI;
        if (isUndefined(tmpURI)) {
            throw new Error('uri undefined');
        }
        return await this.client.requestJSON({
            method: METHOD_POST,
            path: '/finish_torch',
            args: {
                tmp_uri: tmpURI,
                csv_uri: this.getURI(),
                owner_dag: await this.getOwnerDag(),
                owner_node: await this.getOwnerNode(),
                filename: fileName,
            },
        });
    }
}

// *** TorchBlobHandle ***

export class CustomCodeBlobHandle extends BlobHandle {
    public async setCustomImports(
        modules: string[][]
    ): Promise<NodeCustomImports> {
        return await this.client.requestJSON({
            method: METHOD_PUT,
            path: '/custom_imports',
            args: {
                dag: await this.getOwnerDag(),
                node: await this.getOwnerNode(),
                modules,
            },
        });
    }

    public async getCustomImports(): Promise<NodeCustomImports> {
        return await this.client.requestJSON({
            method: METHOD_GET,
            path: '/custom_imports',
            args: {
                dag: await this.getOwnerDag(),
                node: await this.getOwnerNode(),
            },
        });
    }

    public async setCustomCode(
        func: string,
        funcName: string
    ): Promise<NodeCustomCode> {
        const rawCode = formCustomCode(func, funcName);
        return await this.client.requestJSON({
            method: METHOD_PUT,
            path: '/custom_code',
            args: {
                dag: await this.getOwnerDag(),
                node: await this.getOwnerNode(),
                code: rawCode,
            },
        });
    }

    public async setRawCustomCode(rawCode: string): Promise<NodeCustomCode> {
        return await this.client.requestJSON({
            method: METHOD_PUT,
            path: '/custom_code',
            args: {
                dag: await this.getOwnerDag(),
                node: await this.getOwnerNode(),
                code: rawCode,
            },
        });
    }

    public async getCustomCode(): Promise<NodeCustomCode> {
        return await this.client.requestJSON({
            method: METHOD_GET,
            path: '/custom_code',
            args: {
                dag: await this.getOwnerDag(),
                node: await this.getOwnerNode(),
            },
        });
    }
}

// *** CustomCodeBlobHandle ***

export class JSONBlobHandle extends BlobHandle {
    count: number;

    public getCount() {
        return this.count;
    }

    public async appendJSONS(
        jsons: any[],
        requeueOnFinish: NodeHandle = undefined
    ): Promise<JSONBlobHandle> {
        let obj: { [key: string]: any } = {
            blob: this.getURI(),
            jsons: jsons,
        };
        if (!isUndefined(requeueOnFinish)) {
            obj = {
                ...obj,
                dag: requeueOnFinish.getDag().getURI(),
                node: requeueOnFinish.getId(),
            };
        }
        const res = await this.client.requestJSON<JSONBlobAppendResponse>({
            method: METHOD_PUT,
            path: '/json_append',
            args: obj,
        });
        this.count = res.count;
        return this;
    }
}

// *** JSONBlobHandle ***

export class ComputationHandle {
    dag: DagHandle;
    valueId: string;
    value: ByteResponse | undefined = undefined;
    getDynError: () => string | undefined;
    setDynError: (error: string) => void;
    constructor(
        dag: DagHandle,
        valueId: string,
        getDynError: () => string | undefined,
        setDynError: (error: string) => void
    ) {
        this.dag = dag;
        this.valueId = valueId;
        this.getDynError = getDynError;
        this.setDynError = setDynError;
    }

    public hasFetched(): boolean {
        return this.value !== undefined;
    }

    public async get(): Promise<ByteResponse | null> {
        try {
            if (isUndefined(this.value)) {
                const res = await this.dag.getDynamicResult(this.valueId);
                this.value = res;
                return res;
            } else {
                return this.value;
            }
        } catch (error) {
            if (this.getDynError() === undefined) {
                this.setDynError(JSON.stringify(error));
            }
            throw error;
        }
    }

    public getId(): string {
        return this.valueId;
    }
}

// *** ComputationHandle ***
