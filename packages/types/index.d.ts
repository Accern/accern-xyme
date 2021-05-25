/// <reference types="node" />
/// <reference lib="dom" />
import { Readable } from 'stream';
import { promises as fpm } from 'fs';
import { AllowedCustomImports, BlobOwner, BlobTypeResponse, CacheStats, DagDef, DagInfo, DagList, DagPrettyNode, DictStrStr, DynamicFormat, InstanceStatus, KafkaGroup, KafkaOffsets, KafkaThroughput, KafkaTopics, KnownBlobs, MinimalQueueStatsResponse, ModelInfo, ModelParamsResponse, ModelReleaseResponse, NodeCustomImports, NodeDef, NodeDefInfo, NodeInfo, NodeState, NodeTypes, NodeUserColumnsResponse, QueueStatsResponse, QueueStatus, SettingsObj, TaskStatus, Timing, TimingResult, UploadFilesResponse, VersionResponse } from './types';
import { RetryOptions } from './request';
import { ByteResponse } from './util';
export * from './errors';
export * from './request';
export * from './types';
export interface XYMEConfig {
    url: string;
    token: string;
    namespace?: string;
}
interface XYMERequestArgument {
    addNamespace?: boolean;
    addPrefix?: boolean;
    apiVersion?: number;
    args: {
        [key: string]: any;
    };
    files?: {
        [key: string]: Buffer;
    };
    method: string;
    path: string;
    retry?: Partial<RetryOptions>;
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
    private getPrefix;
    private rawRequestBytes;
    private rawRequestJSON;
    private rawRequestString;
    private fallibleRawRequestBytes;
    private fallibleRawRequestJSON;
    private fallibleRawRequestString;
    requestBytes(rargs: XYMERequestArgument): Promise<[Buffer, string]>;
    requestJSON<T>(rargs: XYMERequestArgument): Promise<T>;
    requestString(rargs: XYMERequestArgument): Promise<Readable>;
    getServerVersion(): Promise<VersionResponse>;
    getNamespaces(): Promise<string[]>;
    getDags(): Promise<string[]>;
    getDagAges(): Promise<[string, string, string][]>;
    getDagTimes(retrieveTimes?: boolean): Promise<[DagList['cur_time'], DagList['dags']]>;
    getDag(dagURI: string): Promise<DagHandle>;
    getBlobHandle(uri: string, isFull?: boolean): BlobHandle;
    getNodeDefs(): Promise<NodeTypes['info']>;
    createNewBlob(blobType: string): Promise<string>;
    getBlobOwner(blobURI: string): Promise<BlobOwner>;
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
    setBlobOwner(blobURI: string, dagId?: string, nodeId?: string, externalOwner?: boolean): Promise<BlobOwner>;
    createNewDag(userName?: string, dagName?: string, index?: string): Promise<string>;
    getBlobType(blobURI: string): Promise<BlobTypeResponse>;
    getCSVBlob(blobURI: string): Promise<CSVBlobHandle>;
    getCustomCodeBlob(blobURI: string): Promise<CustomCodeBlobHandle>;
    getJSONBlob(blobURI: string): Promise<JSONBlobHandle>;
    duplicateDag(dagURI: string, destURI?: string, copyNonownedBlobs?: boolean): Promise<string>;
    setDag(dagURI: string, defs: DagDef): Promise<DagHandle>;
    setSettings(settings: SettingsObj): Promise<SettingsObj>;
    getSettings(): Promise<SettingsObj>;
    getAllowedCustomImports(): Promise<AllowedCustomImports>;
    checkQueueStats(minimal: false, dag?: string): Promise<QueueStatsResponse>;
    checkQueueStats(minimal: true, dag?: string): Promise<MinimalQueueStatsResponse>;
    getInstanceStatus(dagURI?: string, nodeId?: string): Promise<{
        [key in InstanceStatus]: number;
    }>;
    getQueueMode(): Promise<string>;
    setQueueMode(mode: string): Promise<string>;
    flushAllQueueData(): Promise<void>;
    getCacheStats(): Promise<CacheStats>;
    resetCache(): Promise<CacheStats>;
    createKafkaErrorTopic(): Promise<KafkaTopics>;
    getKafkaErrorTopic(): Promise<string>;
    deleteKafkaErrorTopic(): Promise<KafkaTopics>;
    readKafkaErrors(offset: string): Promise<string[]>;
    getNamedSecrets(showValues?: boolean): Promise<{
        [key: string]: string | null;
    }>;
    setNamedSecrets(key: string, value: string): Promise<boolean>;
    getErrorLogs(): Promise<string>;
    getKnownBlobAges(blobType?: string, connector?: string): Promise<[string, string][]>;
    getKnonwBlobTimes(retrieveTimes: boolean, blobType?: string, connector?: string): Promise<[KnownBlobs['cur_time'], KnownBlobs['blobs']]>;
    getTritonModels(): Promise<string[]>;
    getUUID(): Promise<string>;
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
    getURI(): string;
    getNodes(): Promise<string[]>;
    getNode(nodeName: string): Promise<NodeHandle>;
    getName(): Promise<string>;
    getCompany(): Promise<string>;
    getStateType(): Promise<string>;
    getTiming(blacklist?: string[]): Promise<TimingResult>;
    isHighPriority(): Promise<boolean>;
    isQueue(): Promise<boolean>;
    getQueueMng(): Promise<string | undefined>;
    getIns(): Promise<string[]>;
    getOuts(): Promise<[string, string][]>;
    setDag(defs: DagDef): Promise<void>;
    dynamicModel(inputs: any[], formatMethod?: DynamicFormat, noCache?: boolean): Promise<any[]>;
    dynamicList(inputs: any[], fargs: {
        inputKey?: string;
        outputKey?: string;
        splitTh?: number | null;
        formatMethod?: DynamicFormat;
        forceKeys?: boolean;
        noCache?: boolean;
    }): Promise<any[]>;
    dynamic(inputData: Buffer): Promise<ByteResponse>;
    dynamicObj(inputObj: any): Promise<ByteResponse>;
    dynamicAsync(inputData: Buffer[]): Promise<ComputationHandle[]>;
    setDynamicErrorMessage(msg?: string): void;
    getDynamicErrorMessage(): string | undefined;
    dynamicAsyncObj(inputData: any[]): Promise<ComputationHandle[]>;
    getDynamicResult(valueId: string): Promise<ByteResponse>;
    getDynamicStatus(valueIds: ComputationHandle[]): Promise<{
        [key: string]: QueueStatus;
    }>;
    private _pretty;
    pretty(nodesOnly?: boolean, allowUnicode?: boolean, method?: string, display?: boolean): Promise<string | undefined>;
    prettyObj(nodesOnly?: boolean, allowUnicode?: boolean): Promise<DagPrettyNode[]>;
    getDef(full?: boolean): Promise<DagDef>;
    setAttr(attr: string, value: any): Promise<void>;
    setName(value: string): Promise<void>;
    setCompany(value: string): Promise<void>;
    setState(value: string): Promise<void>;
    setHighPriority(value: string): Promise<void>;
    setQueueMng(value: string | undefined): Promise<void>;
    checkQueueStats(minimal: false): Promise<QueueStatsResponse>;
    checkQueueStats(minimal: true): Promise<MinimalQueueStatsResponse>;
    scaleWorker(replicas: number): Promise<boolean>;
    reload(timestamp: number | undefined): Promise<number>;
    getKafkaInputTopic(postfix?: string): Promise<string>;
    getKafkaOutputTopic(): Promise<string>;
    setKafkaTopicPartitions(fargs: {
        postfix?: string;
        numPartitions?: number;
        largeInputRetention?: boolean;
        noOutput?: boolean;
    }): Promise<KafkaTopics>;
    postKafkaObjs(inputObjs: any[]): Promise<string[]>;
    postKafkaMsgs(inputData: Buffer[], postfix?: string): Promise<string[]>;
    readKafkaOutput(offset?: string, maxRows?: number): Promise<ByteResponse | null>;
    getKafkaOffsets(alive: boolean, postfix?: string): Promise<KafkaOffsets>;
    getKafkaThroughput(postfix?: string, segmentInterval?: number, segments?: number): Promise<KafkaThroughput>;
    getKafkaGroup(): Promise<KafkaGroup>;
    setKafkaGroup(groupId: string | undefined, reset: string | undefined, ...kwargs: any[]): Promise<KafkaGroup>;
}
export declare class NodeHandle {
    blobs: {
        [key: string]: BlobHandle;
    };
    client: XYMEClient;
    configError?: string;
    dag: DagHandle;
    nodeId: string;
    inputs: {
        [key: string]: [string, string];
    };
    name: string;
    state?: number;
    type: string;
    _isModel?: boolean;
    constructor(client: XYMEClient, dag: DagHandle, nodeId: string, name: string, kind: string);
    asOwner(): BlobOwner;
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
    setBlobURI(key: string, blobURI: string): Promise<string>;
    getInCursorStates(): Promise<{
        [key: string]: number;
    }>;
    getHighestChunk(): Promise<number>;
    getShortStatus(allowUnicode?: boolean): Promise<string>;
    getLogs(): Promise<string>;
    getTiming(): Promise<Timing[]>;
    readBlob(key: string, chunk: number | undefined, forceRefresh?: boolean): Promise<BlobHandle>;
    read(key: string, chunk: number | null, forceRefresh?: boolean): Promise<ByteResponse | null>;
    /**
     * Read and combine all output chunks.
     */
    readAll(key: string, forceRefresh?: boolean): Promise<ByteResponse | null>;
    clear(): Promise<NodeState>;
    getBlobURI(blobKey: string, blobType: string): Promise<[string, BlobOwner]>;
    getCSVBlob(key?: string): Promise<CSVBlobHandle>;
    getJSONBlob(key?: string): Promise<JSONBlobHandle>;
    getCustomCodeBlob(key?: string): Promise<CustomCodeBlobHandle>;
    checkCustomCodeNode(): void;
    getUserColumn(key: string): Promise<NodeUserColumnsResponse>;
    getDef(): Promise<NodeDef>;
    isModel(): Promise<boolean>;
    ensureIsModel(): void;
    setupModel(obj: {
        [key: string]: any;
    }): Promise<ModelInfo>;
    getModelParams(): Promise<ModelParamsResponse>;
}
export declare class BlobHandle {
    client: XYMEClient;
    uri: string;
    isFull: boolean;
    ctype?: string;
    tmpURI?: string;
    owner?: BlobOwner;
    info?: Promise<{
        [key: string]: any;
    }>;
    constructor(client: XYMEClient, uri: string, isFull: boolean);
    isEmpty(): boolean;
    getURI(): string;
    getPath(path: string[]): BlobHandle;
    getContentType(): string | undefined;
    clearInfoCache(): void;
    getInfo(): Promise<{
        [key: string]: any;
    }>;
    getContent(): Promise<ByteResponse | null>;
    private asStr;
    private ensureIsFull;
    private ensureNotFull;
    listFiles(): Promise<BlobHandle[]>;
    setOwner(owner: NodeHandle): Promise<BlobOwner>;
    getOwner(): Promise<BlobOwner>;
    setLocalOwner(owner: BlobOwner): void;
    getOwnerDag(): Promise<string>;
    getOwnerNode(): Promise<string>;
    /**
     * User can pass `externalOwner: true` to set the blob at toURI as
     * external-owned blob.
     */
    copyTo(toURI: string, newOwner: NodeHandle | undefined, externalOwner?: boolean): Promise<BlobHandle>;
    downloadZip(toPath?: string): Promise<Buffer | undefined>;
    performUploadAction(action: string, additional: {
        [key: string]: string | number;
    }, fobj: Buffer | null): Promise<UploadFilesResponse>;
    startUpload(size: number, hashStr: string, ext: string): Promise<string>;
    appendUpload(uri: string, fobj: Buffer): Promise<number>;
    finishUploadZip(): Promise<string[]>;
    clearUpload(): Promise<void>;
    /**
     * This is the helper method being used by uploadFile
     * and uploadFileUsingContent
     * @param curSize: Size of the updated buffer so far
     * @param buffer: the buffer chunk being uploaded
     * @param nread: number of bytes from the in the buffer
     * @param chunk: chunk size
     * @param blobHandle: the parent this being passed here
     * @returns
     */
    updateBuffer(curSize: number, buffer: Buffer, nread: number, chunk: number, blobHandle: BlobHandle): Promise<number>;
    uploadFile(fileContent: fpm.FileHandle, ext: string, progressBar?: WritableStream): Promise<void>;
    /**
     * This prototype method allows you to upload the file using content Buffer
     * @param contentBuffer: file content as Buffer object
     * @param ext: The file extension (e.g .csv)
     * @param progressBar: stream where we show the upload progress
     */
    uploadFileUsingContent(contentBuffer: Buffer, ext: string, progressBar?: WritableStream): Promise<void>;
    uploadZip(source: string | fpm.FileHandle): Promise<BlobHandle[]>;
    convertModel(reload?: boolean): Promise<ModelReleaseResponse>;
}
export declare class CSVBlobHandle extends BlobHandle {
    addFromFile(fileName: string, progressBar?: WritableStream | undefined): Promise<UploadFilesResponse>;
    addFromContent(fileName: string, content: Buffer, progressBar?: WritableStream | undefined): Promise<UploadFilesResponse>;
    finishCSVUpload(fileName?: string): Promise<UploadFilesResponse>;
}
export declare class CustomCodeBlobHandle extends BlobHandle {
    setCustomImports(modules: string[][]): Promise<NodeCustomImports>;
    getCustomImports(): Promise<NodeCustomImports>;
}
export declare class JSONBlobHandle extends BlobHandle {
    count: number;
    getCount(): number;
    appendJSONS(jsons: any[], requeueOnFinish?: NodeHandle): Promise<JSONBlobHandle>;
}
export declare class ComputationHandle {
    dag: DagHandle;
    valueId: string;
    value: ByteResponse | undefined;
    getDynError: () => string | undefined;
    setDynError: (error: string) => void;
    constructor(dag: DagHandle, valueId: string, getDynError: () => string | undefined, setDynError: (error: string) => void);
    hasFetched(): boolean;
    get(): Promise<ByteResponse>;
    getId(): string;
}
