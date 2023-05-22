export interface URIPrefix {
    connector: string;
    address: string;
}

export interface BaseDagDef {
    name: string;
    company: string;
    high_priority: boolean;
    nodes: Partial<NodeDef>[];
    queue_mng?: string;
}

export interface UserDagDef extends BaseDagDef {
    uri_prefix?: URIPrefix;
    state_uri?: string;
    default_input_key?: string;
    default_output_key?: string;
    kafka_input_topic?: string;
    kafka_output_topic?: string;
    version_override?: string;
}

export interface DagDef extends BaseDagDef {
    default_input_key?: string;
    default_output_key?: string;
    kafka_input_topic?: string;
    kafka_output_topic?: string;
    state_uri: string;
    uri_prefix: URIPrefix;
    version_override: string;
}

export interface NodeDef {
    blobs: DictStrStr;
    id: string;
    inputs: { [key: string]: [string, string] };
    kind: string;
    name: string;
    params: { [key: string]: any };
    version_override?: string;
}

export interface S3BucketSettings {
    api_version?: string;
    aws_access_key_id: string;
    aws_secret_access_key: string;
    aws_session_token?: string;
    buckets: DictStrStr;
    endpoint_url: string;
    region_name: string;
    use_ssl?: boolean;
    verify?: boolean;
}

export interface ESConnectorSettings {
    host: string;
    password: string;
}

export interface DremioAuthSettings {
    host: string;
    user: string;
    password: string;
}

export interface SettingsObj {
    es?: { [key: string]: ESConnectorSettings };
    s3?: { [key: string]: S3BucketSettings };
    triton?: { [key: string]: S3BucketSettings };
    dremio?: { [key: string]: DremioAuthSettings };
    versions?: { [key: string]: [string, string] };
}

export type TaskType = 'node:cpubig' | 'node:cpusmall';

export type QueueType = 'parallel:kafka' | 'parallel:queue';

export type QueueStatus = 'waiting' | 'running' | 'result' | 'error' | 'void';

export type TaskStatus =
    | 'blocked'
    | 'waiting'
    | 'running'
    | 'complete'
    | 'eos'
    | 'paused'
    | 'error'
    | 'unknown'
    | 'virtual'
    | 'queue';

export type ParamType =
    | 'bool'
    | 'choice_list'
    | 'choice_set'
    | 'choice'
    | 'col'
    | 'date'
    | 'int'
    | 'list_col'
    | 'list_str'
    | 'list_tup_str'
    | 'mapping_col_num'
    | 'mapping_col_type'
    | 'mapping_int_int'
    | 'mapping_str_str'
    | 'mapping_str_tup_str_int'
    | 'mapping_tup_int_float'
    | 'mapping_tup_int_tup_int_int_int_int'
    | 'mapping_tup_str_str'
    | 'num'
    | 'opt_col'
    | 'opt_coltype'
    | 'opt_date'
    | 'opt_int'
    | 'opt_scol'
    | 'opt_str'
    | 'pair_num'
    | 'scol'
    | 'str'
    | 'col_or_mapping_str_col';

export type InstanceStatus =
    | 'busy_queue'
    | 'busy_task'
    | 'data_queue'
    | 'off'
    | 'ready_queue'
    | 'ready_task';

export interface ParamDef {
    name: string;
    help: string;
    type: ParamType;
    required: boolean;
    default?: string | string[] | number;
}

export interface ParamDefs {
    [key: string]: ParamDef;
}

export interface ModelParamDefs {
    [key: string]: ParamDefs;
}

export interface Backends {
    logger: string[];
    status_emitter: string[];
    progress: string;
    queue_mng: DictStrStr;
    task_mng: DictStrStr;
    executor_mng: string;
    mtype: string;
    ns: string;
}

export type BlobType =
    | 'bucket_buff'
    | 'context'
    | 'csr_matrix'
    | 'csv'
    | 'custom_code'
    | 'dag'
    | 'data'
    | 'data_adaptive'
    | 'datalike'
    | 'dataresult'
    | 'embedding_model'
    | 'es'
    | 'json'
    | 'raw'
    | 'seq_buff'
    | 'sklearn_model'
    | 'sorting_buff'
    | 'text_embedding_model'
    | 'text_model_clf'
    | 'text_model_reg'
    | 'tmp'
    | 'torch';

export interface VersionResponse {
    api_version: number;
    api_version_minor: number;
    backends?: Backends;
    caller_api_version: number;
    image_repo?: string;
    image_tag?: string;
    time: string;
    xyme_version_full: string;
    xyme_version: string;
}

export interface KafkaErrorMessageState {
    msgLookup: Map<string, string>;
    unmatched: string[];
}

export interface ReadNode {
    redis_key: string;
    result_uri?: string;
}

export interface NodeDefInfo {
    name: string;
    desc: string;
    input_keys: string[];
    output_keys: string[];
    task_types?: TaskType[];
    queue_types?: QueueType[];
    blob_types: Record<string, string | string[]>;
    params: ParamDefs;
    version_override?: string;
}

export interface NodeStatus {
    status: TaskStatus;
}

export interface NodeChunk {
    chunk: number;
}

export interface NodeState {
    status: TaskStatus;
    chunk: number;
}

export interface NodeTypes {
    types: string[];
    info: { [key: string]: NodeDefInfo };
}

export interface NodeInfo {
    id: string;
    name: string;
    type: string;
    blobs: DictStrStr;
    inputs: { [key: string]: [string, string] };
    state?: number;
    config_error?: string;
    version_override?: string;
}
export interface DagStatus {
    config_error?: string;
    created?: number;
    dag: string;
    deleted?: number;
    latest?: number;
    oldest?: number;
}
export interface DagList {
    cur_time: number;
    dags: DagStatus[];
}

export interface KnownBlobs {
    cur_time: number;
    blobs: [string, number | null][];
}

export interface DagInfo {
    company: string;
    high_priority: boolean;
    ins: string[];
    kafka_input_topic?: string;
    kafka_output_topic?: string;
    name: string;
    nodes: NodeInfo[];
    outs: [string, string][];
    queue_mng?: string;
    uri_prefix: URIPrefix;
    state_uri: string;
    version_override?: string;
}

export interface BlobInit {
    blob: string;
}

export interface DagInit {
    dag: string;
}

export interface DagCreate {
    dag: string;
    nodes: string[];
    warnings: string[];
}

export interface NamespaceUpdateSettings {
    namespace: string;
    settings: SettingsObj;
}

export interface DagReload {
    dag: string;
    when: number;
}

export interface Timing {
    name: string;
    total: number;
    quantity: number;
    avg: number;
}

export interface Timings {
    times: Timing[];
}

export interface NodeTiming {
    nodeName: string;
    nodeTotal: number;
    nodeAvg: number;
    fns: Timing[];
}

export interface TimingResult {
    dagTotal: number;
    nodes: [string, NodeTiming][];
}

export interface InCursors {
    cursors: { [key: string]: number };
}

export interface CSVBlobResponse {
    count: number;
    csv: string;
    pos: number;
    tmp: boolean;
}

export interface CSVOp {
    count: number;
    pos: number;
    tmp: boolean;
}

export interface NamespaceList {
    namespaces: string[];
}

export interface NodeCustomCode {
    code: string;
}

export interface NodeCustomImports {
    modules: string[][];
}

export interface AllowedCustomImports {
    modules: string[];
}

export interface JSONBlobResponse {
    count: number;
    json: string;
}

export interface JSONBlobAppendResponse {
    count: number;
}

export interface NodeUserColumnsResponse {
    user_columns: string[];
}

export interface ModelParamsResponse {
    model_params: ModelParamDefs;
}

export interface ModelInfo {
    model_info: DictStrStr;
}

export interface BlobFilesResponse {
    files: string[];
}

export interface UploadFilesResponse {
    uri?: string;
    pos: number;
    files: string[];
}

export interface DagDupResponse {
    dag: string;
}

export interface FlushAllQueuesResponse {
    success: boolean;
}

export interface WorkerScale {
    num_replicas: number;
}

export interface SetNamedSecret {
    replaced: boolean;
}

export interface KafkaTopics {
    topics: { [key: string]: string | undefined };
    create: boolean;
}

export interface KafkaTopicNames {
    input: string | null;
    output: string | null;
    error: string | null;
    error_msg: string | null;
}

export interface KafkaMessage {
    messages: DictStrStr;
}

export interface KafkaOffsets {
    error_msg: number;
    error: number;
    input: number;
    output: number;
}

export interface KafkaGroup {
    group: string;
    dag: string;
    reset?: string;
}

export interface ThroughputDict {
    throughput: number;
    max: number;
    min: number;
    stddev: number;
    segments: number;
    count: number;
    total: number;
}

export interface KafkaThroughput {
    dag: string;
    input: ThroughputDict;
    output: ThroughputDict;
    faster: 'input' | 'output' | 'both';
    errors: number;
    errorMsgs: number;
}

export interface PutNodeBlob {
    key: string;
    new_uri: string;
}

export interface BlobOwner {
    owner_dag?: string;
    owner_node: string;
}

export interface CopyBlob {
    new_uri: string;
}

export interface QueueStatsResponse {
    active: number;
    error: number;
    extras: { [key: string]: number };
    need: boolean;
    queue_count: number;
    restarted: number;
    results: number;
    start_blocker: boolean;
    threshold: number;
    total: number;
    workers: number;
}

export interface MinimalQueueStatsResponse {
    active: number;
    total: number;
}

export interface DynamicStatusResponse {
    status: { [key: string]: QueueStatus };
}

export interface DynamicResults {
    results: any[];
}

export interface TritonModelsResponse {
    models: string[];
}

export interface CacheStats {
    total: number;
}

export interface QueueMode {
    mode: string;
}

export interface ModelReleaseResponse {
    release?: number;
}

export interface ModelVersionResponse {
    max_version?: number;
    all_versions: string[];
}

export interface ESQueryResponse {
    query: { [key: string]: any };
}

export interface DagPrettyEdge {
    out_name: string;
    in_node: string;
    in_name: string;
    in_state: number;
    move_right: number;
    move_down: number;
}

export interface DagPrettyNode {
    id: string;
    name: string;
    kind: string;
    info: { [key: string]: number | string };
    high_ixs: { [key: string]: number };
    status: TaskStatus;
    out_edges: DagPrettyEdge[];
    indent: number;
}

export interface PrettyResponse {
    pretty: string;
    nodes: DagPrettyNode[];
}

// =====================

export type DictStrStr = { [key: string]: string };

export type DictStrList = { [key: string]: string[] };

export type DynamicFormat = 'simple' | 'titan';

export interface BlobTypeResponse {
    type: string;
    is_csv: boolean;
    is_custom_code: boolean;
    is_json: boolean;
    is_torch: boolean;
}

export interface BlobURIResponse {
    uri: string;
    owner: BlobOwner;
}
export interface UUIDResponse {
    uuid: string;
}

export interface NodeTypeResponse {
    is_model: boolean;
}
export interface DeleteBlobStatus {
    uri: string;
    status: string;
    deletion_time: number | null;
}
export interface DeleteBlobResponse {
    blobs: DeleteBlobStatus;
}
