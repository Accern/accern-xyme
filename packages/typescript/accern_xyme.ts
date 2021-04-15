/// <reference lib="dom" />
import { Readable } from 'stream';
import { open, FileHandle } from 'fs/promises';
import { performance } from 'perf_hooks';
import {
    AllowedCustomImports,
    BlobFilesResponse,
    BlobInit,
    BlobOwner,
    CacheStats,
    CopyBlob,
    CSVBlobResponse,
    CSVOp,
    DagCreate,
    DagDef,
    DagInfo,
    DagInit,
    DagList,
    DagReload,
    DictStrStr,
    DynamicFormat,
    DynamicResults,
    FlushAllQueuesResponse,
    InCursors,
    InstanceStatus,
    KafkaGroup,
    KafkaMessage,
    KafkaOffsets,
    KafkaThroughput,
    KafkaTopics,
    KnownBlobs,
    MinimalQueueStatsResponse,
    ModelReleaseResponse,
    NamespaceUpdateSettings,
    NodeChunk,
    NodeDefInfo,
    NodeInfo,
    NodeState,
    NodeStatus,
    NodeTiming,
    NodeTypes,
    PutNodeBlob,
    QueueMode,
    QueueStatsResponse,
    ReadNode,
    SetNamedSecret,
    SettingsObj,
    TaskStatus,
    Timing,
    TimingResult,
    Timings,
    TritonModelsResponse,
    VersionResponse,
    WorkerScale,
} from './types';
import {
    HTTPResponseError,
    METHOD_FILE,
    METHOD_GET,
    METHOD_POST,
    METHOD_PUT,
    rawRequestBytes,
    rawRequestJSON,
    rawRequestString,
    RequestArgument,
    RetryOptions,
} from './request';
import {
    assertBoolean,
    assertString,
    forceKey,
    getAge,
    getFileHash,
    getFileUploadChunkSize,
    isUndefined,
    KeyError,
    openWrite,
    safeOptNumber,
    std,
    useLogger,
} from './util';

const API_VERSION = 4;
const EMPTY_BLOB_PREFIX = 'null://';
const PREFIX = 'xyme';
const logger = useLogger('xyme');
const DEFAULT_NAMESPACE = 'default';

export interface XYMEConfig {
    url: string;
    token: string;
    namespace?: string;
}

interface XYMERequestArgument extends Partial<RequestArgument> {
    files?: { [key: string]: any };
    addNamespace?: boolean;
    addPrefix?: boolean;
    args: { [key: string]: any };
    method: string;
    path: string;
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
        if (typeof this.apiVersion === undefined) {
            this.apiVersion = await this.getServerVersion().then((response) => {
                if (response.api_version < API_VERSION) {
                    throw new Error(`Legacy version ${response.api_version}`);
                }
                return response.api_version;
            });
        }
        if (this.apiVersion === undefined) {
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

    public async requestJSON<T>(rargs: XYMERequestArgument): Promise<T> {
        const {
            addNamespace = true,
            addPrefix = true,
            path,
            args,
            method,
            ...rest
        } = rargs;
        const targs = { ...args };
        if (addNamespace) {
            targs.namespace = this.namespace;
        }
        let URL = `${this.url}${path}`;
        if (addPrefix) {
            URL = `${this.url}${PREFIX}/v${API_VERSION}${path}`;
        }

        const requestArgs: RequestArgument = {
            ...rest,
            method,
            args,
            URL,
            headers: {
                'Authorization': this.token,
                'content-type': 'application/json',
            },
        };
        return await rawRequestJSON<T>(requestArgs);
    }

    public async requestString(rargs: XYMERequestArgument): Promise<Readable> {
        const {
            addNamespace = true,
            addPrefix = true,
            path,
            args,
            method,
            ...rest
        } = rargs;
        const targs = { ...args };
        if (addNamespace) {
            targs.namespace = this.namespace;
        }
        let URL = `${this.url}${path}`;
        if (addPrefix) {
            URL = `${this.url}${PREFIX}/v${API_VERSION}${path}`;
        }
        const requestArgs: RequestArgument = {
            ...rest,
            method,
            args,
            URL,
            headers: {
                'Authorization': this.token,
                'content-type': 'application/text',
            },
        };
        return await rawRequestString(requestArgs);
    }

    public async requestBytes(
        rargs: XYMERequestArgument
    ): Promise<[Buffer, string]> {
        const {
            addNamespace = true,
            addPrefix = true,
            path,
            args,
            method,
            ...rest
        } = rargs;
        const targs = { ...args };
        if (addNamespace) {
            targs.namespace = this.namespace;
        }
        let URL = `${this.url}${path}`;
        if (addPrefix) {
            URL = `${this.url}${PREFIX}/v${API_VERSION}${path}`;
        }
        const requestArgs: RequestArgument = {
            ...rest,
            method,
            args,
            URL,
            headers: {
                'Authorization': this.token,
                'content-type': 'application/json',
            },
        };
        return await rawRequestBytes(requestArgs);
    }

    public async getServerVersion(): Promise<VersionResponse> {
        return await this.requestJSON<VersionResponse>({
            method: METHOD_GET,
            path: `${PREFIX}/v${API_VERSION}/verion`,
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
        const [cur_time, dags] = await this.getDagTimes(false);
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
                getAge(cur_time, dag[1]),
                getAge(cur_time, dag[2]),
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
        destUri?: string
    ): Promise<string> {
        return await this.requestJSON<DagCreate>({
            method: METHOD_POST,
            path: '/dag_dup',
            args: {
                dag: dagUri,
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
            logger.info(`
                ${numWarnings} warnings while setting dag ${dagUri}:\n"`);
        } else if (numWarnings === 1) {
            logger.info(`Warning while setting dag ${dagUri}:\n`);
        }
        warnings.forEach((warn) => logger.info(`${warn}\n`));
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
        if (minimal) {
            return await this.requestJSON<MinimalQueueStatsResponse>({
                method: METHOD_GET,
                path: '/queue_stats',
                args: {
                    dag,
                    minimal: 1,
                },
            });
        } else {
            return this.requestJSON<QueueStatsResponse>({
                method: METHOD_GET,
                path: '/queue_stats',
                args: {
                    dag,
                    minimal: 0,
                },
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

    public async resetCacheStats(): Promise<CacheStats> {
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
        showValues: false
    ): Promise<{ [key: string]: string | null }> {
        return await this.requestJSON<{ [key: string]: string | null }>({
            method: METHOD_GET,
            path: '/named_secrets',
            args: {
                show: +showValues,
            },
        });
    }

    public async setNamedSecrets(key: string, value: string): Promise<boolean> {
        return await this.requestJSON<SetNamedSecret>({
            method: METHOD_PUT,
            path: '/named_secrets',
            args: {
                key,
                value,
            },
        }).then((response) => response.replaced);
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
            ([, a], [, b]) => a['node_total'] - b['node_total']
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

    // public async dynamicList(
    //     inputs: any[],
    //     inputKey: string | undefined,
    //     outputKey: string | undefined,
    //     splitTh = 1000,
    //     maxThreads = 50,
    //     formatMethod: DynamicFormat.simple,
    //     forceKeys = false,
    //     noCache = false,
    // ): Promise<any[]> {

    // }

    public async dynamic(inputData: Buffer): Promise<Buffer> {
        return await this.client.requestBytes({
            method: METHOD_FILE,
            path: '/dynamic',
            args: {
                dag: this.getUri(),
            },
            files: {
                file: inputData,
            },
        })[0];
    }

    public async dynamicObj(inputObj: any): Promise<Buffer> {
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

    public async getDynamicResult(valueId: string): Promise<Buffer> {
        try {
            return this.client.requestBytes({
                method: METHOD_GET,
                path: 'dynamic_result',
                args: {
                    dag: this.getUri(),
                    id: valueId,
                },
            })[0];
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

    public async pretty() {
        return await this.client.requestString({
            method: METHOD_GET,
            path: '/dag_pretty',
            args: {
                dag: this.getUri(),
            },
        });
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

    public async reload(timestamp: number = undefined): Promise<number> {
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
        const names = inputData.reduce(
            (acc, _cV, cIndex) => [...acc, `file${cIndex}`],
            []
        );
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
        return names.reduce((acc, name) => [...acc, msgs[name]], []);
    }

    public async readKafkaOutput(
        offset = 'current',
        maxRows = 100
    ): Promise<Buffer | null> {
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
            return await readSingle()[0];
        }
        let res: Buffer[] = [];
        let ctype: string = undefined;
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

        if (res.length === 0) {
            return null;
        }
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
                // eslint-disable-next-line @typescript-eslint/no-empty-function
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
    id: string;
    inputs: { [key: string]: [string, string] } = {};
    name: string;
    state?: number;
    type: string;

    constructor(
        client: XYMEClient,
        dag: DagHandle,
        id: string,
        name: string,
        kind: string
    ) {
        this.client = client;
        this.dag = dag;
        this.id = id;
        this.name = name;
        this.type = kind;
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
        if (this.id !== nodeInfo.id) {
            throw new Error(`${this.id} != ${nodeInfo.id}`);
        }
        this.name = nodeInfo.name;
        this.type = nodeInfo.type;
        this.inputs = nodeInfo.inputs;
        this.state = nodeInfo.state;
        this.configError = nodeInfo.config_error;
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
        return this.id;
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
        return new BlobHandle(this.client, uri, true);
    }

    public async read(
        key: string,
        chunk: number | undefined,
        forceRefresh?: boolean
    ): Promise<Buffer | null> {
        const blob = await this.readBlob(key, chunk, forceRefresh || false);
        const content = await blob.getContent();
        return content;
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
        return new CSVBlobHandle(
            this.client,
            res.csv,
            res.count,
            res.pos,
            res.tmp
        );
    }
}

export class BlobHandle {
    client: XYMEClient;
    uri: string;
    isFull: boolean;
    ctype?: string;

    constructor(client: XYMEClient, uri: string, isFull: boolean) {
        this.client = client;
        this.uri = uri;
        this.isFull = isFull;
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

    public async getContent(): Promise<Buffer | null> {
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
                return await this.client.requestBytes({
                    method: METHOD_POST,
                    path: '/uri',
                    args: {
                        uri: this.getUri(),
                    },
                    retry: retryOptions,
                })[0];
            } catch (error) {
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
        const ownerNode = newOwner !== undefined ? newOwner.getId() : undefined;
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
        return new BlobHandle(this.client, response.new_uri, false);
    }

    public async downloadZip(toPath?: string): Promise<Buffer | undefined> {
        if (this.isFull) {
            throw new Error(`URI must not be full: ${this.getUri()}`);
        }
        const res = await this.client.requestBytes({
            method: METHOD_GET,
            path: '/download_zip',
            args: {
                blob: this.getUri(),
            },
        })[0];
        if (isUndefined(toPath)) {
            return res;
        }
        await openWrite(res, toPath);
        return;
    }

    public async performAction(
        action: string,
        additional: { [key: string]: string | number },
        fobj: Buffer | null,
        requeueOnFinish: NodeHandle | undefined = undefined
    ): Promise<number> {
        let args: { [key: string]: string | number } = {
            action,
        };
        if (action === 'start') {
            args = {
                ...args,
                target: this.getUri(),
            };
        } else {
            args = {
                ...args,
                uri: this.getUri(),
            };
        }
        if (requeueOnFinish !== undefined && action == 'finish') {
            args = {
                ...args,
                dag: requeueOnFinish.getDag().getUri(),
                node: requeueOnFinish.getId(),
            };
        }
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
        args = {
            ...args,
            ...additional,
        };
        await this.client.requestJSON<CSVOp>({
            method,
            path: '/upload_file',
            args: {
                ...args,
                file_type: 'csv',
            },
            files,
        });
        return 0;
    }

    public async startData(size: number, hashStr: string): Promise<number> {
        return await this.performAction(
            'start',
            {
                target: this.getUri(),
                hash: hashStr,
                size,
            },
            null
        );
    }
    public async appendData(fobj: Buffer): Promise<number> {
        return this.performAction('append', {}, fobj);
    }

    public async finishData(
        requeueOnFinish: NodeHandle | undefined = undefined
    ) {
        await this.performAction('finish', {}, null, requeueOnFinish);
    }

    public async clearTmp() {
        await this.performAction('clear', {}, null);
    }

    public async uploadData(
        fileContent: FileHandle,
        requeueOnFinish: NodeHandle | undefined,
        progressBar: WritableStream | undefined
    ): Promise<number> {
        const totalSize = (await fileContent.stat()).size;
        if (progressBar !== undefined) {
            progressBar.getWriter().write('Uploading file:\n');
        }
        const fileHash = await getFileHash(fileContent);

        let curSize = await this.startData(totalSize, fileHash);
        let curPos = 0;
        async function uploadNextChunk(
            that: BlobHandle,
            chunk: number,
            fileHandle: FileHandle
        ): Promise<void> {
            const buffer = Buffer.alloc(chunk);
            await fileHandle.read(buffer, 0, 0, 0);
            const response = await fileHandle.read(buffer, 0, chunk, curPos);
            const nread = response.bytesRead;
            curPos += nread;
            if (nread === 0) {
                return;
            }
            let data: Buffer;
            if (nread < chunk) {
                data = buffer.slice(0, nread);
            } else {
                data = buffer;
            }
            const newSize = await that.appendData(data);
            if (newSize - curSize !== data.length) {
                throw new Error(`
                    incomplete chunk upload n:${newSize} o:${curSize}
                    b: ${data.length}
                `);
            }
            curSize = newSize;
            await uploadNextChunk(that, chunk, fileHandle);
        }
        await uploadNextChunk(this, getFileUploadChunkSize(), fileContent);
        await this.finishData(requeueOnFinish);
        return curSize;
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

export class CSVBlobHandle extends BlobHandle {
    count: number;
    pos: number;
    hasTmp: boolean;

    constructor(
        client: XYMEClient,
        uri: string,
        count: number,
        pos: number,
        hasTmp: boolean
    ) {
        super(client, uri, false);
        this.count = count;
        this.pos = pos;
        this.hasTmp = hasTmp;
    }

    public getCount(): number {
        return this.count;
    }

    public getPos(): number {
        return this.pos;
    }

    public async addFromFile(
        fileName: string,
        requeueOnFinish: NodeHandle | undefined = undefined,
        progressBar: WritableStream | undefined = undefined
    ) {
        const fileHandle = await open(fileName, 'r');

        try {
            await this.uploadData(fileHandle, requeueOnFinish, progressBar);
        } finally {
            await fileHandle.close();
        }
    }
}

export class ComputationHandle {
    dag: DagHandle;
    valueId: string;
    value: Buffer | undefined = undefined;
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

    public async get(): Promise<Buffer> {
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
