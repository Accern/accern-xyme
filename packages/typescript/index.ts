/// <reference lib="dom" />
import { Readable } from 'stream';
import FormData from 'form-data';
import { promises as fpm } from 'fs';
import { HeadersInit, Response, RequestInit } from 'node-fetch';
import { performance } from 'perf_hooks';
import jsSHA from 'jssha';
import {
    AllowedCustomImports,
    BlobFilesResponse,
    BlobInit,
    BlobOwner,
    CacheStats,
    CopyBlob,
    CSVBlobResponse,
    DagCreate,
    DagDef,
    DagInfo,
    DagInit,
    DagList,
    DagPrettyNode,
    DagReload,
    DictStrStr,
    DynamicFormat,
    DynamicResults,
    DynamicStatusResponse,
    FlushAllQueuesResponse,
    InCursors,
    InstanceStatus,
    JSONBlobAppendResponse,
    // JSONBlobResponse,
    KafkaGroup,
    KafkaMessage,
    KafkaOffsets,
    KafkaThroughput,
    KafkaTopics,
    KnownBlobs,
    MinimalQueueStatsResponse,
    ModelParamsResponse,
    ModelReleaseResponse,
    ModelSetupResponse,
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
    UploadFilesResponse,
    VersionResponse,
    WorkerScale,
} from './types';
import {
    HTTPResponseError,
    METHOD_FILE,
    METHOD_GET,
    METHOD_POST,
    METHOD_PUT,
    retryWithTimeout,
    RetryOptions,
    handleError,
    METHOD_DELETE,
} from './request';
import {
    assertBoolean,
    assertString,
    ByteResponse,
    forceKey,
    getAge,
    getFileHash,
    getFileUploadChunkSize,
    getQueryURL,
    interpretCtype,
    isUndefined,
    mergeCtype,
    openWrite,
    safeOptNumber,
    std,
} from './util';
import { KeyError } from './errors';
export * from './errors';
export * from './request';
export * from './types';

const API_VERSION = 4;
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

export default class XYMEClient {
    apiVersion?: number;
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
    }

    public async getAPIVersion(): Promise<number> {
        if (isUndefined(this.apiVersion)) {
            const serverVersions = await this.getServerVersion();

            if (serverVersions.api_version < API_VERSION) {
                throw new Error(
                    `Legacy version ${serverVersions.api_version}`
                );
            }
            this.apiVersion = serverVersions.api_version;
        }
        if (isUndefined(this.apiVersion)) {
            throw new Error('no apiVersion');
        }
        return this.apiVersion;
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
            method,
            path,
            args,
            addPrefix,
            addNamespace,
            apiVersion,
            files,
            retry,
            ...rest
        } = rargs;
        const prefix = await this.getPrefix(addPrefix, apiVersion);
        const url = `${this.url}${prefix}${path}`;
        let headers: HeadersInit = {
            Authorization: this.token,
        };
        if (addNamespace) {
            headers = {
                ...headers,
                namespace: this.namespace,
            };
        }
        const options: RequestInit = {
            method,
            headers,
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
                    getQueryURL(args, url),
                    retry,
                    init
                );
                break;
            }
            case METHOD_POST:
            case METHOD_PUT:
            case METHOD_DELETE: {
                response = await retryWithTimeout(url, retry, options);
                break;
            }
            case METHOD_FILE: {
                const { headers } = options;
                const formData = new FormData();
                if (files) {
                    Object.keys(files).map((key) => {
                        const buffCopy = Buffer.from(files[key]);
                        formData.append(key, buffCopy);
                    });
                    Object.keys(args).map((key) => {
                        formData.append(key, args[key]);
                    });
                    response = await retryWithTimeout(url, retry, {
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
            return [
                await response.buffer(),
                response.headers.get('content-type'),
            ];
        } else {
            throw new Error('no server response');
        }
    }

    private async rawRequestJSON<T>(rargs: XYMERequestArgument): Promise<T> {
        const {
            method,
            path,
            args,
            addPrefix,
            addNamespace,
            apiVersion,
            files,
            retry,
            ...rest
        } = rargs;
        const prefix = await this.getPrefix(addPrefix, apiVersion);
        const url = `${this.url}${prefix}${path}`;
        let headers: HeadersInit = {
            'Authorization': this.token,
            'content-type': 'application/json',
        };
        if (addNamespace) {
            headers = {
                ...headers,
                namespace: this.namespace,
            };
        }
        const options: RequestInit = {
            method,
            headers,
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
                    getQueryURL(args, url),
                    retry,
                    init
                );
                break;
            }
            case METHOD_POST:
            case METHOD_PUT:
            case METHOD_DELETE: {
                response = await retryWithTimeout(url, retry, options);
                break;
            }
            case METHOD_FILE: {
                const { headers } = options;
                const formData = new FormData();
                if (files) {
                    Object.keys(files).map((key) => {
                        const buffCopy = Buffer.from(files[key]);
                        formData.append(key, buffCopy);
                    });
                    Object.keys(args).map((key) => {
                        formData.append(key, args[key]);
                    });
                    response = await retryWithTimeout(url, retry, {
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

    private async rawRequestString(
        rargs: XYMERequestArgument
    ): Promise<Readable> {
        const {
            method,
            path,
            args,
            addPrefix,
            addNamespace,
            apiVersion,
            retry,
            ...rest
        } = rargs;
        const prefix = await this.getPrefix(addPrefix, apiVersion);
        const url = `${this.url}${prefix}${path}`;
        let response: Response = undefined;
        let headers: HeadersInit = {
            'Authorization': this.token,
            'content-type': 'application/text',
        };
        if (addNamespace) {
            headers = {
                ...headers,
                namespace: this.namespace,
            };
        }
        const options: RequestInit = {
            ...rest,
            method,
            headers,
            body: JSON.stringify(args),
        };

        switch (method) {
            case METHOD_GET: {
                // eslint-disable-next-line @typescript-eslint/no-unused-vars
                const { body, ...init } = options;
                response = await retryWithTimeout(
                    getQueryURL(args, url),
                    retry,
                    init
                );
                break;
            }
            case METHOD_POST:
                response = await retryWithTimeout(url, retry, options);
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
        return dags.map((dag) => dag[0]);
    }

    public async getDagAges(): Promise<[string, string, string][]> {
        const [curTime, dags] = await this.getDagTimes(true);
        const sorted = dags.sort((a, b) => {
            const oldA = safeOptNumber(a[1]);
            const oldB = safeOptNumber(b[1]);
            let cmp = +oldA[0] - +oldB[0] || oldA[1] - oldB[1];
            if (cmp !== 0) {
                return cmp;
            }
            const latestA = safeOptNumber(a[2]);
            const latestB = safeOptNumber(b[2]);
            cmp = +latestA[0] - +latestB[0] || latestA[1] - latestB[1];
            if (cmp !== 0) {
                return cmp;
            }
            return +(a[0] >= b[0]);
        });
        const ages: [string, string, string][] = [];
        sorted.forEach((dag) => {
            ages.push([
                dag[0],
                getAge(curTime, dag[1]),
                getAge(curTime, dag[2]),
            ]);
        });
        return ages;
    }
    public async getDagTimes(
        retrieveTimes: boolean
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

    public async getDag(dagUri: string): Promise<DagHandle> {
        const mDag = this.dagCache.get({ uri: dagUri });
        if (mDag) {
            return mDag;
        }
        const dag = new DagHandle(this, dagUri);
        this.dagCache.set({ uri: dagUri }, dag);
        return dag;
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

    // public getBlobHandle(uri: string, isFull = false): BlobHandle {
    //     return new BlobHandle(this, uri, isFull);
    // }

    public async getCSVBlobHandle(blobURI: string): Promise<CSVBlobHandle> {
        const owner = await this.requestJSON<BlobOwner>({
            method: METHOD_GET,
            path: '/blob_owner',
            args: {
                blob: blobURI,
            },
        });
        return new CSVBlobHandle(this, blobURI, owner);
    }

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

    public async duplicateDag(
        dagUri: string,
        destUri?: string,
        copyNonownedBlobs = true
    ): Promise<string> {
        return await this.requestJSON<DagCreate>({
            method: METHOD_POST,
            path: '/dag_dup',
            args: {
                dag: dagUri,
                copy_nonowned_blobs: copyNonownedBlobs,
                ...(destUri ? { dest: destUri } : {}),
            },
        }).then((response) => response.dag);
    }

    public async setDag(dagUri: string, defs: DagDef): Promise<DagHandle> {
        const dagCreate = await this.requestJSON<DagCreate>({
            method: METHOD_POST,
            path: '/dag_create',
            args: {
                dag: dagUri,
                defs,
            },
        });
        const uri = dagCreate.dag;
        const warnings = dagCreate.warnings;
        const numWarnings = warnings.length;
        if (numWarnings > 1) {
            console.info(`
                ${numWarnings} warnings while setting dag ${dagUri}:\n"`);
        } else if (numWarnings === 1) {
            console.info(`Warning while setting dag ${dagUri}:\n`);
        }
        warnings.forEach((warn) => console.info(`${warn}\n`));
        return this.getDag(uri);
    }

    public async setSettings(settings: SettingsObj): Promise<SettingsObj> {
        return await this.requestJSON<NamespaceUpdateSettings>({
            method: METHOD_POST,
            path: '/settings',
            args: {
                settings,
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
        let args: { [key: string]: number | string } = {
            minimal: +minimal,
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
        dagUri?: string,
        nodeId?: string
    ): Promise<{ [key in InstanceStatus]: number }> {
        return await this.requestJSON<{ [key in InstanceStatus]: number }>({
            method: METHOD_GET,
            path: '/instance_status',
            args: {
                dag: dagUri,
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

    public async deleteKafkaErrorTopic(): Promise<KafkaTopics> {
        return await this.requestJSON<KafkaTopics>({
            method: METHOD_POST,
            path: '/kafka_topics',
            args: {
                num_partitions: 0,
            },
        });
    }

    public async readKafkaErrors(offset: string): Promise<string[]> {
        return await this.requestJSON<string[]>({
            method: METHOD_GET,
            path: '/kafka_msg',
            args: {
                offset: offset || 'current',
            },
        });
    }

    public async getNamedSecrets(
        showValues = false
    ): Promise<{ [key: string]: string | null }> {
        return await this.requestJSON<{ [key: string]: string | null }>({
            method: METHOD_GET,
            path: '/named_secrets',
            args: {
                show: +showValues,
            },
        });
    }

    public async setNamedSecrets(
        key: string,
        value: string
    ): Promise<boolean> {
        return await this.requestJSON<SetNamedSecret>({
            method: METHOD_PUT,
            path: '/named_secrets',
            args: {
                key,
                value,
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
}

export class DagHandle {
    client: XYMEClient;
    company?: string;
    dynamicError?: string;
    ins?: string[];
    highPriority?: boolean;
    name?: string;
    nodeLookup: DictStrStr = {};
    nodes: { [key: string]: NodeHandle } = {};
    outs?: [string, string][];
    queueMng?: string;
    state?: string;
    uri: string;

    constructor(client: XYMEClient, uri: string) {
        this.client = client;
        this.uri = uri;
    }

    public refresh() {
        this.company = undefined;
        this.ins = undefined;
        this.highPriority = undefined;
        this.name = undefined;
        this.outs = undefined;
        this.queueMng = undefined;
        this.state = undefined;
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
        this.state = info.state;
        this.highPriority = info.high_priority;
        this.queueMng = info.queue_mng;
        this.ins = info.ins;
        this.outs = info.outs;
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

    public getUri(): string {
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

    public async getStateType(): Promise<string> {
        this.maybeRefresh();
        await this.maybeFetch();
        return assertString(this.state);
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

    public setDag(defs: DagDef) {
        this.client.setDag(this.getUri(), defs);
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
                    dag: this.getUri(),
                },
            })
            .then((response) => response.results);
    }

    public async dynamicList(
        inputs: any[],
        inputKey: string = undefined,
        outputKey: string = undefined,
        splitTh: number | null = 1000,
        formatMethod: DynamicFormat = 'simple',
        forceKeys = false,
        noCache = false
    ): Promise<any[]> {
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
                        dag: this.getUri(),
                    },
                })
                .then((res) => res.results);
        }
        const resArray: any[] = new Array(inputs.length).fill(null);
        const splitNum = splitTh;
        const computeHalf = async (cur: any[], offset: number) => {
            if (cur.length <= splitNum) {
                const curRes = await this.dynamicList(
                    cur,
                    inputKey,
                    outputKey,
                    null,
                    formatMethod,
                    forceKeys,
                    noCache
                );
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

    public async dynamic(inputData: Buffer): Promise<ByteResponse> {
        const [res, ctype] = await this.client.requestBytes({
            method: METHOD_FILE,
            path: '/dynamic',
            args: {
                dag: this.getUri(),
            },
            files: {
                file: inputData,
            },
        });
        return interpretCtype(res, ctype);
    }

    public async dynamicObj(inputObj: any): Promise<ByteResponse> {
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
                dag: this.getUri(),
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

    public async getDynamicResult(valueId: string): Promise<ByteResponse> {
        try {
            const [res, ctype] = await this.client.requestBytes({
                method: METHOD_GET,
                path: '/dynamic_result',
                args: {
                    dag: this.getUri(),
                    id: valueId,
                },
            });
            return interpretCtype(res, ctype);
        } catch (error) {
            if (
                error instanceof HTTPResponseError &&
                error.response.status === 404
            ) {
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
                dag: this.getUri(),
            },
        });
        const status = res.status;
        let hndMap: { [key: string]: ComputationHandle } = {};
        valueIds.map((id) => {
            hndMap = {
                ...hndMap,
                [id.getId()]: id,
            };
        });
        let hndStatus: { [key: string]: QueueStatus } = {};
        Object.keys(status).map((key) => {
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
        prettyMethod = 'accern'
    ): Promise<PrettyResponse> {
        return await this.client.requestJSON<PrettyResponse>({
            method: METHOD_GET,
            path: '/pretty',
            args: {
                dag: this.getUri(),
                nodes_only: nodesOnly,
                allow_unicode: allowUnicode,
                method: prettyMethod,
            },
        });
    }

    public async pretty(
        nodesOnly = false,
        allowUnicode = true,
        prettyMethod = 'accern',
        display = true
    ): Promise<string | undefined> {
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
            prettyMethod
        ).then((res) => res.pretty);
        return render(graphStr);
    }

    public async prettyObj(
        nodesOnly = false,
        allowUnicode = true
    ): Promise<DagPrettyNode[]> {
        return await this._pretty(nodesOnly, allowUnicode).then(
            (res) => res.nodes
        );
    }

    public async getDef(full = true): Promise<DagDef> {
        return await this.client.requestJSON({
            method: METHOD_GET,
            path: '/dag_def',
            args: {
                dag: this.getUri(),
                full: +full,
            },
        });
    }

    public async setAttr(attr: string, value: any): Promise<void> {
        let dagDef = await this.getDef();
        dagDef = {
            ...dagDef,
            [attr]: value,
        };
        await this.client.setDag(this.getUri(), dagDef);
    }

    public async setName(value: string): Promise<void> {
        await this.setAttr('name', value);
    }

    public async setCompany(value: string): Promise<void> {
        await this.setAttr('company', value);
    }

    public async setState(value: string): Promise<void> {
        await this.setAttr('state', value);
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
            return await this.client.checkQueueStats(true, this.getUri());
        } else {
            return await this.client.checkQueueStats(false, this.getUri());
        }
    }

    public async scaleWorker(replicas: number): Promise<boolean> {
        return await this.client
            .requestJSON<WorkerScale>({
                method: METHOD_POST,
                path: '/worker',
                args: {
                    dag: this.getUri(),
                    replicas,
                    task: undefined,
                },
            })
            .then((res) => res.success);
    }

    public async reload(timestamp: number | undefined): Promise<number> {
        return await this.client
            .requestJSON<DagReload>({
                method: METHOD_POST,
                path: '/dag_reload',
                args: {
                    dag: this.getUri(),
                    when: timestamp,
                },
            })
            .then((res) => res.when);
    }

    public async setKafkaTopicPartitions(
        numPartitions: number,
        largeInputRetention = false
    ): Promise<KafkaTopics> {
        return await this.client.requestJSON<KafkaTopics>({
            method: METHOD_POST,
            path: '/kafka_to[ics',
            args: {
                dag: this.getUri(),
                num_partitions: numPartitions,
                large_input_retention: largeInputRetention,
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

    public async postKafkaMsgs(inputData: Buffer[]): Promise<string[]> {
        const range = Array.from(Array(inputData.length).keys());
        const names: string[] = range.map((ix) => `file_${ix}`);

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
                    dag: this.getUri(),
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
                    dag: this.getUri(),
                    offset: offsetStr[0],
                },
            });
        };
        if (maxRows <= 1) {
            const [res, ctype] = await readSingle();
            return interpretCtype(res, ctype);
        }
        let res: Buffer[] = [];
        let ctype: string | undefined;
        // eslint-disable-next-line no-constant-condition
        while (true) {
            const [val, curCtype] = await readSingle();
            if (val === null) {
                break;
            }
            if (curCtype === null) {
                ctype = curCtype;
            } else if (ctype !== curCtype) {
                throw new Error(
                    `inconsistent return types ${ctype} != ${curCtype}`
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
        return mergeCtype(res, ctype);
    }

    public async getKafkaOffsets(alive: boolean): Promise<KafkaOffsets> {
        return await this.client.requestJSON({
            method: METHOD_GET,
            path: '/kafka_offsets',
            args: {
                dag: this.getUri(),
                alive: +alive,
            },
        });
    }

    public async getKafkaThroughput(
        segmentInterval = 120.0,
        segments = 5
    ): Promise<KafkaThroughput> {
        if (segmentInterval <= 0) {
            throw new Error('segmentInterval should be > 0');
        }
        if (segments <= 0) {
            throw new Error('segments should be > 0');
        }
        let offsets = await this.getKafkaOffsets(false);
        let now = performance.now();
        let measurements: [number, number, number, number][] = [
            [offsets.input, offsets.output, offsets.error, now],
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
                [offsets.input, offsets.output, offsets.error, now],
            ];
        });
        const first = measurements[0];
        const last = measurements[-1];
        const totalInput = last[0] - first[0];
        const totalOutput = last[1] - first[1];
        const errors = last[2] - first[2];
        const total = last[3] - first[3];
        let inputSegments: number[] = [];
        let outputSegments: number[] = [];
        let curInput = first[0];
        let curOutput = first[1];
        let curTime = first[3];
        measurements.slice(1).forEach((measurement) => {
            const [nextInput, nextOutput, , nextTime] = measurement;
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
            dag: this.getUri(),
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
        };
    }

    public async getKafkaGroup(): Promise<KafkaGroup> {
        return await this.client.requestJSON({
            method: METHOD_GET,
            path: '/kafka_group',
            args: {
                dag: this.getUri(),
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
                dag: this.getUri(),
                groupId,
                reset,
                ...kwargs,
            },
        });
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
    type: string;

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

    private asOwner(): BlobOwner {
        return {
            owner_dag: this.getDag().getUri(),
            owner_node: this.getId(),
        } as BlobOwner;
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
        const blobs = nodeInfo.blobs;
        const owner = this.asOwner();
        this.blobs = Object.keys(blobs).reduce(
            (o, key) => ({
                ...o,
                [key]: new BlobHandle(this.client, blobs[key], false, owner),
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
                    dag: this.getDag().getUri(),
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

    public async setBlobUri(key: string, blobUri: string): Promise<string> {
        return await this.client
            .requestJSON<PutNodeBlob>({
                method: METHOD_PUT,
                path: '/node_blob',
                args: {
                    dag: this.getDag().getUri(),
                    node: this.getId(),
                    blob_key: key,
                    blob_uri: blobUri,
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
                    dag: this.getDag().getUri(),
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
                    dag: this.getDag().getUri(),
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
                dag: this.getDag().getUri(),
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
                    dag: this.getDag().getUri(),
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
                    dag: this.getDag().getUri(),
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
        return new BlobHandle(this.client, uri, true, this.asOwner());
    }

    public async read(
        key: string,
        chunk: number | undefined,
        forceRefresh?: boolean
    ): Promise<ByteResponse | null> {
        const blob = await this.readBlob(key, chunk, forceRefresh || false);
        return await blob.getContent();
    }

    public async clear(): Promise<NodeState> {
        return this.client.requestJSON<NodeState>({
            method: METHOD_PUT,
            path: '/node_state',
            args: {
                dag: this.getDag().getUri(),
                node: this.getId(),
                action: 'reset',
            },
        });
    }

    public async getCSVBlob(): Promise<CSVBlobHandle> {
        if (this.getType() !== 'csv_reader') {
            throw new Error('node has no csv blob');
        }
        const res = await this.client.requestJSON<CSVBlobResponse>({
            method: METHOD_GET,
            path: '/csv_blob',
            args: {
                dag: this.getDag().getUri(),
                node: this.getId(),
            },
        });
        const owner: BlobOwner = {
            owner_dag: this.getDag().getUri(),
            owner_node: this.getId(),
        };
        return new CSVBlobHandle(this.client, res.csv, owner);
    }

    // public async getJSONBlob(): Promise<JSONBlobHandle> {
    //     if (this.getType() !== 'jsons_reader') {
    //         throw new Error('node has no json blob');
    //     }
    //     const res = await this.client.requestJSON<JSONBlobResponse>({
    //         method: METHOD_GET,
    //         path: '/json_blob',
    //         args: {
    //             dag: this.getDag().getUri(),
    //             node: this.getId(),
    //         },
    //     });
    //     return new JSONBlobHandle(this.client, res['json'], res['count']);
    // }

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
                dag: this.getDag().getUri(),
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
                dag: this.getDag().getUri(),
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
    tmpUri?: string;
    owner: BlobOwner;

    constructor(
        client: XYMEClient,
        uri: string,
        isFull: boolean,
        owner: BlobOwner
    ) {
        this.client = client;
        this.uri = uri;
        this.isFull = isFull;
        this.owner = owner;
    }

    public getOwnerDag(): string {
        return this.owner.owner_dag;
    }

    public getOwnerNode(): string {
        return this.owner.owner_node;
    }

    private isEmpty(): boolean {
        return this.uri.startsWith(EMPTY_BLOB_PREFIX);
    }

    public getUri(): string {
        return this.uri;
    }

    public getCtype(): string | undefined {
        return this.ctype;
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
                        uri: this.getUri(),
                    },
                    retry: retryOptions,
                });
                return interpretCtype(res, ctype);
            } catch (error) {
                if (
                    error instanceof HTTPResponseError &&
                    error.response.status === 404
                ) {
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
            return new BlobHandle(this.client, blob, true, this.owner);
        });
    }

    public async setOwner(owner: NodeHandle): Promise<BlobOwner> {
        this.ensureNotFull();
        return this.client.requestJSON<BlobOwner>({
            method: METHOD_PUT,
            path: '/blob_owner',
            args: {
                blob: this.uri,
                owner_dag: owner.getDag().getUri(),
                owner_node: owner.getId(),
            },
        });
    }

    public async getOwner(): Promise<BlobOwner> {
        this.ensureNotFull();
        return this.client.requestJSON<BlobOwner>({
            method: METHOD_GET,
            path: '/blob_owner',
            args: {
                blob: this.uri,
            },
        });
    }

    public async copyTo(
        toUri: string,
        newOwner: NodeHandle | undefined
    ): Promise<BlobHandle> {
        this.ensureNotFull();
        const ownerDag =
            newOwner !== undefined ? newOwner.getDag().getUri() : undefined;
        const ownerNode =
            newOwner !== undefined ? newOwner.getId() : undefined;
        const response = await this.client.requestJSON<CopyBlob>({
            method: METHOD_POST,
            path: '/copy_blob',
            args: {
                from_uri: this.uri,
                owner_dag: ownerDag,
                owner_node: ownerNode,
                to_uri: toUri,
            },
        });
        const owner = {
            owner_dag: ownerDag,
            owner_node: ownerNode,
        } as BlobOwner;
        return new BlobHandle(this.client, response.new_uri, false, owner);
    }

    public async downloadZip(toPath?: string): Promise<Buffer | undefined> {
        if (this.isFull) {
            throw new Error(`URI must not be full: ${this.getUri()}`);
        }
        const [res] = await this.client.requestBytes({
            method: METHOD_GET,
            path: '/download_zip',
            args: {
                blob: this.getUri(),
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
            this.tmpUri = undefined;
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
                target: this.getUri(),
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
    public async appendUpload(uri: string, fobj: Buffer): Promise<number> {
        const res = await this.performUploadAction('append', { uri }, fobj);
        return res.pos;
    }

    public async finishUploadZip(): Promise<string[]> {
        const uri = this.tmpUri;
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
        const uri = this.tmpUri;
        if (!isUndefined(uri)) {
            await this.performUploadAction('clear', { uri }, null);
        }
    }
    /**
     * This is the helper method being used by uploadFile
     * and uploadFileUsingContent
     * @param curSize: Size of the updated buffer so far
     * @param buffer: the buffer chunk being uploaded
     * @param nread: number of bytes from the in the buffer
     * @param chunk: chunk size
     * @param that: the parent this being passed here
     * @returns
     */
    public async updateBuffer(
        curSize: number,
        buffer: Buffer,
        nread: number,
        chunk: number,
        that: BlobHandle
    ) {
        let data: Buffer;
        if (nread < chunk) {
            data = buffer.slice(0, nread);
        } else {
            data = buffer;
        }
        const newSize = await that.appendUpload(this.tmpUri, data);
        if (newSize - curSize !== data.length) {
            throw new Error(`
                incomplete chunk upload n:${newSize} o:${curSize}
                b: ${data.length}
            `);
        }
        return newSize;
    }
    public async uploadFile(
        fileContent: fpm.FileHandle,
        ext: string,
        progressBar?: WritableStream
    ): Promise<void> {
        const totalSize = (await fileContent.stat()).size;
        if (progressBar !== undefined) {
            progressBar.getWriter().write('Uploading file:\n');
        }
        const fileHash = await getFileHash(fileContent);

        const tmpUri = await this.startUpload(totalSize, fileHash, ext);
        this.tmpUri = tmpUri;
        let curPos = 0;
        let curSize = 0;

        async function uploadNextChunk(
            that: BlobHandle,
            chunk: number,
            fileHandle: fpm.FileHandle
        ): Promise<void> {
            const buffer = Buffer.alloc(chunk);
            await fileHandle.read(buffer, 0, 0, 0);
            const response = await fileHandle.read(buffer, 0, chunk, curPos);
            const nread = response.bytesRead;
            if (!nread) {
                return;
            }
            curPos += nread;
            const newSize = await this.updateBuffer(
                curSize,
                buffer,
                nread,
                chunk,
                that
            );
            curSize = newSize;
            await uploadNextChunk(that, chunk, fileHandle);
        }

        await uploadNextChunk(this, getFileUploadChunkSize(), fileContent);
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
        if (progressBar !== undefined) {
            progressBar.getWriter().write('Uploading file:\n');
        }
        const totalSize = contentBuffer.byteLength;
        const shaObj = new jsSHA('SHA-224', 'BYTES');
        shaObj.update(contentBuffer.toString());
        const fileHash = shaObj.getHash('HEX');
        const tmpUri = await this.startUpload(totalSize, fileHash, ext);
        this.tmpUri = tmpUri;
        let curPos = 0;
        let curSize = 0;
        async function uploadNextBufferChunk(
            that: BlobHandle,
            chunk: number,
            contentBuffer: Buffer
        ): Promise<void> {
            const buffer = contentBuffer.slice(curPos, chunk);
            const nread = buffer.byteLength;
            if (!nread) {
                return;
            }
            curPos += nread;
            const newSize = await that.updateBuffer(
                curSize,
                buffer,
                nread,
                chunk,
                that
            );
            curSize = newSize;
            await uploadNextBufferChunk(that, chunk, contentBuffer);
        }

        await uploadNextBufferChunk(
            this,
            getFileUploadChunkSize(),
            contentBuffer
        );
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
            (blobUri) => new BlobHandle(this.client, blobUri, true, this.owner)
        );
    }
}

export class CSVBlobHandle extends BlobHandle {
    constructor(client: XYMEClient, uri: string, owner: BlobOwner) {
        super(client, uri, false, owner);
    }

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
            return await this.finishCSVUpload();
        } finally {
            await fileHandle.close();
            await this.clearUpload();
        }
    }

    public async addFromContent(
        fileName: string,
        content: Buffer,
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

        try {
            await this.uploadFileUsingContent(content, ext, progressBar);
            return await this.finishCSVUpload();
        } finally {
            await this.clearUpload();
        }
    }

    public async finishCSVUpload(): Promise<UploadFilesResponse> {
        const tmpUri = this.tmpUri;
        if (isUndefined(tmpUri)) {
            throw new Error('uri undefined');
        }
        return await this.client.requestJSON({
            method: METHOD_POST,
            path: '/finish_csv',
            args: {
                tmp_uri: tmpUri,
                csv_uri: this.getUri(),
                owner_dag: this.owner.owner_dag,
                owner_node: this.owner.owner_node,
            },
        });
    }
}

export class CustomCodeBlobHandle extends BlobHandle {
    constructor(client: XYMEClient, uri: string, owner: BlobOwner) {
        super(client, uri, false, owner);
    }

    public async setCustomImports(
        modules: string[][]
    ): Promise<NodeCustomImports> {
        return await this.client.requestJSON({
            method: METHOD_PUT,
            path: '/custom_imports',
            args: {
                dag: this.getOwnerDag(),
                node: this.getOwnerNode(),
                modules,
            },
        });
    }

    public async getCustomImports(): Promise<NodeCustomImports> {
        return await this.client.requestJSON({
            method: METHOD_GET,
            path: '/custom_imports',
            args: {
                dag: this.getOwnerDag(),
                node: this.getOwnerNode(),
            },
        });
    }
}

export class ModelBlobHandle extends BlobHandle {
    constructor(client: XYMEClient, uri: string, owner: BlobOwner) {
        super(client, uri, false, owner);
    }
    public async setupModel(obj: {
        [key: string]: any;
    }): Promise<ModelSetupResponse> {
        return await this.client.requestJSON<ModelSetupResponse>({
            method: METHOD_PUT,
            path: '/model_setup',
            args: {
                dag: this.getOwnerDag(),
                node: this.getOwnerNode(),
                config: obj,
            },
        });
    }

    public async getModelParams(): Promise<ModelParamsResponse> {
        return await this.client.requestJSON<ModelParamsResponse>({
            method: METHOD_GET,
            path: '/model_params',
            args: {
                dag: this.getOwnerDag(),
                node: this.getOwnerNode(),
            },
        });
    }

    public async convertModel(): Promise<ModelReleaseResponse> {
        return await this.client.requestJSON<ModelReleaseResponse>({
            method: METHOD_POST,
            path: '/convert_model',
            args: {
                blob: this.getUri(),
            },
        });
    }
}

export class JSONBlobHandle extends BlobHandle {
    count: number;
    constructor(
        client: XYMEClient,
        uri: string,
        count: number,
        owner: BlobOwner
    ) {
        super(client, uri, false, owner);
        this.count = count;
    }

    public getCount() {
        return this.count;
    }

    public async appendJSONS(
        jsons: any[],
        requeueOnFinish: NodeHandle = undefined
    ): Promise<JSONBlobHandle> {
        let obj: { [key: string]: any } = {
            blob: this.getUri(),
            jsons: jsons,
        };
        if (!isUndefined(requeueOnFinish)) {
            obj = {
                ...obj,
                dag: requeueOnFinish.getDag().getUri(),
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

    public async get(): Promise<ByteResponse> {
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
