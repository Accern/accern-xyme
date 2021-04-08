/// <reference types="node" />
/// <reference lib="dom" />
import { Readable } from 'stream';
import { FileHandle } from 'fs/promises';
import { AllowedCustomImports, BlobOwner, CacheStats, DagDef, DagInfo, DagList, DictStrStr, DynamicFormat, InstanceStatus, KafkaTopics, KnownBlobs, MinimalQueueStatsResponse, ModelReleaseResponse, NodeDefInfo, NodeInfo, NodeState, NodeTypes, QueueStatsResponse, SettingsObj, TaskStatus, Timing, VersionResponse } from './types';
import { RequestArgument } from './request';
export interface XYMEConfig {
    url: string;
    token: string;
    namespace?: string;
}
interface XYMERequestArgument extends Partial<RequestArgument> {
    files?: {
        [key: string]: any;
    };
    addNamespace?: boolean;
    addPrefix?: boolean;
    args: {
        [key: string]: any;
    };
    method: string;
    path: string;
}
export default class XYMEClient {
    apiVersion?: number;
    autoRefresh: boolean;
    dagCache: WeakMap<{
        ['uri']: string;
    }, DagHandle>;
    namespace: string;
    nodeDefs?: {
        [key: string]: NodeDefInfo;
    };
    token: string;
    url: string;
    constructor(config: XYMEConfig);
    getAPIVersion(): Promise<number>;
    private setAutoRefresh;
    isAutoRefresh(): boolean;
    refresh(): void;
    private maybeRefresh;
    requestJSON<T>(rargs: XYMERequestArgument): Promise<T>;
    requestString(rargs: XYMERequestArgument): Promise<Readable>;
    requestBytes(rargs: XYMERequestArgument): Promise<Buffer>;
    getServerVersion(): Promise<VersionResponse>;
    getNamespaces(): Promise<string[]>;
    getDags(): Promise<string[]>;
    getDagAges(): Promise<[string, string, string][]>;
    getDagTimes(retrieveTimes: boolean): Promise<[DagList['cur_time'], DagList['dags']]>;
    getDag(dagUri: string): Promise<DagHandle>;
    getBlobHandle(uri: string, isFull?: boolean): BlobHandle;
    getNodeDefs(): Promise<NodeTypes['info']>;
    createNewBlob(blobType: string): Promise<string>;
    createNewDag(userName?: string, dagName?: string, index?: string): Promise<string>;
    duplicateDag(dagUri: string, destUri?: string): Promise<string>;
    setDag(dagUri: string, defs: DagDef): Promise<DagHandle>;
    setSettings(settings: SettingsObj): Promise<SettingsObj>;
    getSettings(): Promise<SettingsObj>;
    getAllowedCustomImports(): Promise<AllowedCustomImports>;
    checkQueueStats(minimal: false, dag?: string): Promise<QueueStatsResponse>;
    checkQueueStats(minimal: true, dag?: string): Promise<MinimalQueueStatsResponse>;
    getInstanceStatus(dagUri?: string, nodeId?: string): Promise<{
        [key in InstanceStatus]: number;
    }>;
    getQueueMode(): Promise<string>;
    setQueueMode(mode: string): Promise<string>;
    flushAllQueueData(): Promise<void>;
    getCacheStats(): Promise<CacheStats>;
    resetCacheStats(): Promise<CacheStats>;
    createKafkaErrorTopic(): Promise<KafkaTopics>;
    deleteKafkaErrorTopic(): Promise<KafkaTopics>;
    readKafkaErrors(offset: string): Promise<string[]>;
    getNamedSecrets(showValues: false): Promise<{
        [key: string]: string | null;
    }>;
    setNamedSecrets(key: string, value: string): Promise<boolean>;
    getKnownBlobAges(blobType?: string, connector?: string): Promise<[string, string][]>;
    getKnonwBlobTimes(retrieveTimes: boolean, blobType?: string, connector?: string): Promise<[KnownBlobs['cur_time'], KnownBlobs['blobs']]>;
    getTritonModels(): Promise<string[]>;
}
export declare class DagHandle {
    client: XYMEClient;
    company?: string;
    dynamicError?: string;
    ins?: string[];
    highPriority?: boolean;
    name?: string;
    nodeLookup: DictStrStr;
    nodes: {
        [key: string]: NodeHandle;
    };
    outs?: [string, string][];
    queueMng?: string;
    state?: string;
    uri: string;
    constructor(client: XYMEClient, uri: string);
    refresh(): void;
    private maybeRefresh;
    private maybeFetch;
    private fetchInfo;
    getInfo(): Promise<DagInfo>;
    getUri(): string;
    getNodes(): Promise<string[]>;
    getNode(nodeName: string): Promise<NodeHandle>;
    getName(): Promise<string>;
    getCompany(): Promise<string>;
    getStateType(): Promise<string>;
    isHighPriority(): Promise<boolean>;
    isQueue(): Promise<boolean>;
    getQueueMng(): Promise<string | undefined>;
    getIns(): Promise<string[]>;
    getOuts(): Promise<[string, string][]>;
    setDag(defs: DagDef): void;
    dynamicModel(inputs: any[], formatMethod?: DynamicFormat, noCache?: boolean): Promise<any[]>;
    getDef(full?: boolean): Promise<DagDef>;
    dynamic(inputData: Buffer): Promise<Buffer>;
    dynamic_obj(inputObj: any): Promise<Buffer>;
    dynamicAsync(inputData: Buffer[]): Promise<ComputationHandle[]>;
    setDynamicErrorMessage(msg?: string): void;
    getDynamicErrorMessage(): string | undefined;
    dynamicAsyncObj(inputData: any[]): Promise<ComputationHandle[]>;
    getDynamicResult(valueId: string): Promise<Buffer>;
}
export declare class NodeHandle {
    blobs: {
        [key: string]: BlobHandle;
    };
    client: XYMEClient;
    configError?: string;
    dag: DagHandle;
    id: string;
    inputs: {
        [key: string]: [string, string];
    };
    name: string;
    state?: number;
    type: string;
    constructor(client: XYMEClient, dag: DagHandle, id: string, name: string, kind: string);
    static fromNodeInfo(client: XYMEClient, dag: DagHandle, nodeInfo: NodeInfo, prev?: NodeHandle): NodeHandle;
    private updateInfo;
    getDag(): DagHandle;
    getId(): string;
    getName(): string;
    getType(): string;
    getNodeDef(): Promise<NodeDefInfo>;
    getInputs(): Set<string>;
    getInput(key: string): Promise<[NodeHandle, string]>;
    getStatus(): Promise<TaskStatus>;
    hasConfigError(): boolean;
    getBlobs(): string[];
    getBlobHandles(): {
        [key: string]: BlobHandle;
    };
    getBlobHandle(key: string): BlobHandle;
    setBlobUri(key: string, blobUri: string): Promise<string>;
    getInCursorStates(): Promise<{
        [key: string]: number;
    }>;
    getHighestChunk(): Promise<number>;
    getShortStatus(allowUnicode?: boolean): Promise<string>;
    getLogs(): Promise<string>;
    getTimings(): Promise<Timing[]>;
    readBlob(key: string, chunk: number | undefined, forceRefresh?: boolean): Promise<BlobHandle>;
    read(key: string, chunk: number | undefined, forceRefresh?: boolean): Promise<Buffer | null>;
    clear(): Promise<NodeState>;
    getCSVBlob(): Promise<CSVBlobHandle>;
}
export declare class BlobHandle {
    client: XYMEClient;
    uri: string;
    isFull: boolean;
    ctype?: string;
    constructor(client: XYMEClient, uri: string, isFull: boolean);
    private isEmpty;
    getUri(): string;
    getCtype(): string | undefined;
    getContent(): Promise<Buffer | null>;
    private asStr;
    private ensureIsFull;
    private ensureNotFull;
    listFiles(): Promise<BlobHandle[]>;
    setOwner(owner: NodeHandle): Promise<BlobOwner>;
    getOwner(): Promise<BlobOwner>;
    copyTo(toUri: string, newOwner: NodeHandle | undefined): Promise<BlobHandle>;
    downloadZip(toPath?: string): Promise<Buffer | undefined>;
    performAction(action: string, additional: {
        [key: string]: string | number;
    }, fobj: Buffer | null, requeueOnFinish?: NodeHandle | undefined): Promise<number>;
    startData(size: number, hashStr: string): Promise<number>;
    appendData(fobj: Buffer): Promise<number>;
    finishData(requeueOnFinish?: NodeHandle | undefined): Promise<void>;
    clearTmp(): Promise<void>;
    uploadData(fileContent: FileHandle, requeueOnFinish: NodeHandle | undefined, progressBar: WritableStream | undefined): Promise<number>;
    convertModel(): Promise<ModelReleaseResponse>;
}
export declare class CSVBlobHandle extends BlobHandle {
    count: number;
    pos: number;
    hasTmp: boolean;
    constructor(client: XYMEClient, uri: string, count: number, pos: number, hasTmp: boolean);
    getCount(): number;
    getPos(): number;
    addFromFile(fileName: string, requeueOnFinish?: NodeHandle | undefined, progressBar?: WritableStream | undefined): Promise<void>;
}
export declare class ComputationHandle {
    dag: DagHandle;
    valueId: string;
    value: Buffer | undefined;
    getDynError: () => string | undefined;
    setDynError: (error: string) => void;
    constructor(dag: DagHandle, valueId: string, getDynError: () => string | undefined, setDynError: (error: string) => void);
    hasFetched(): boolean;
    get(): Promise<Buffer>;
    getId(): string;
}
export {};
