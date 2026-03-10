import { registerFileAtWorker, unregisterFileAtWorker, ioNotifier } from "@tursodatabase/database-wasm-common"
import { DatabasePromise } from "@tursodatabase/database-common"
import { ProtocolIo, run, DatabaseOpts, EncryptionOpts, RunOpts, DatabaseRowMutation, DatabaseRowStatement, DatabaseRowTransformResult, DatabaseStats, SyncEngineGuards, Runner, runner, RemoteWriter, RemoteWriteStatement, S3PageClient } from "@tursodatabase/sync-common";
import { SyncEngine, SyncEngineProtocolVersion, initThreadPool, MainWorker, Database as NativeDatabase, EncryptionCipher, decryptS3PageMetadata } from "./index-turbopack-hack.js";

const S3_PAGE_INFO_SUFFIX = "-s3-page-info";
const S3_PAGE_SEGMENT_MAGIC = "TS3P";
const SQLITE_PAGE_SIZE_OFFSET = 16;
const SQLITE_PAGE_SIZE_LENGTH = 2;

let BrowserIO: ProtocolIo = {
    async read(path: string): Promise<Buffer | Uint8Array | null> {
        const result = localStorage.getItem(path);
        if (result == null) {
            return null;
        }
        return new TextEncoder().encode(result);
    },
    async write(path: string, data: Buffer | Uint8Array): Promise<void> {
        const array = new Uint8Array(data);
        const value = new TextDecoder('utf-8').decode(array);
        localStorage.setItem(path, value);
    }
};

function memoryIO(): ProtocolIo {
    let values = new Map();
    return {
        async read(path: string): Promise<Buffer | Uint8Array | null> {
            return values.get(path);
        },
        async write(path: string, data: Buffer | Uint8Array): Promise<void> {
            values.set(path, data);
        }
    }
};

async function init(): Promise<Worker> {
    await initThreadPool();
    if (MainWorker == null) {
        throw new Error("panic: MainWorker is not initialized");
    }
    return MainWorker;
}

function resolveUrl(url: string | (() => string | null)): string {
    if (typeof url === "function") {
        const resolved = url();
        if (resolved == null) {
            throw new Error("remoteWritesExperimental requires a non-null URL");
        }
        return resolved;
    }
    return url;
}

interface S3PageHeadManifest {
    version: number;
    database_id: string;
    head_commit_id: string;
    sequence_no: number;
}

interface S3PageSegmentRef {
    segment_id: string;
    page_ids: number[];
}

interface S3PageCommitManifest {
    version: number;
    database_id: string;
    commit_id: string;
    parent_commit_id?: string | null;
    sequence_no: number;
    page_count: number;
    client_id: string;
    transaction_id: string;
    segments: S3PageSegmentRef[];
}

interface S3PageLocalMetadata {
    version: number;
    database_id: string;
    client_id: string;
    head_commit_id?: string | null;
    sequence_no: number;
    head_etag?: string | null;
    last_pulled_at?: number | null;
    pending_publish_commit_id?: string | null;
    selector_id?: string | null;
    selector_hash?: string | null;
    selector_version?: string | null;
}

interface S3PageDecodedSegment {
    pageId: number;
    bytes: Uint8Array;
}

function s3PageInfoPath(path: string): string {
    return `${path}${S3_PAGE_INFO_SUFFIX}`;
}

function decodeUtf8Json<T>(bytes: Uint8Array): T {
    const copy = new Uint8Array(bytes.byteLength);
    copy.set(bytes);
    return JSON.parse(new TextDecoder().decode(copy)) as T;
}

function currentUnixSeconds(): number {
    return Math.floor(Date.now() / 1000);
}

function parseSqlitePageSize(page: Uint8Array): number | null {
    if (page.length < SQLITE_PAGE_SIZE_OFFSET + SQLITE_PAGE_SIZE_LENGTH) {
        return null;
    }
    const pageSize = (page[SQLITE_PAGE_SIZE_OFFSET] << 8) | page[SQLITE_PAGE_SIZE_OFFSET + 1];
    if (pageSize === 1) {
        return 65536;
    }
    return pageSize > 0 ? pageSize : null;
}

function decodeS3PageSegment(payload: Uint8Array): S3PageDecodedSegment[] {
    const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
    let offset = 0;

    if (payload.byteLength < 12) {
        throw new Error("invalid S3 page segment: payload is too short");
    }
    const magic = new TextDecoder().decode(payload.subarray(0, 4));
    if (magic !== S3_PAGE_SEGMENT_MAGIC) {
        throw new Error(`invalid S3 page segment magic: ${magic}`);
    }
    offset += 4;

    const version = view.getUint32(offset, false);
    offset += 4;
    if (version !== 1) {
        throw new Error(`unsupported S3 page segment version: ${version}`);
    }

    const pageCount = view.getUint32(offset, false);
    offset += 4;
    const pages: S3PageDecodedSegment[] = [];
    for (let index = 0; index < pageCount; index += 1) {
        if (offset + 8 > payload.byteLength) {
            throw new Error("invalid S3 page segment: truncated page header");
        }
        const pageId = view.getUint32(offset, false);
        offset += 4;
        const byteLength = view.getUint32(offset, false);
        offset += 4;
        if (pageId === 0) {
            throw new Error("invalid S3 page segment: page ids must be positive");
        }
        if (offset + byteLength > payload.byteLength) {
            throw new Error("invalid S3 page segment: truncated page body");
        }
        pages.push({
            pageId,
            bytes: payload.subarray(offset, offset + byteLength),
        });
        offset += byteLength;
    }
    if (offset !== payload.byteLength) {
        throw new Error("invalid S3 page segment: trailing bytes detected");
    }
    return pages;
}

function bytesToHex(bytes: Uint8Array): string {
    return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function decodeBase64(base64: string): Uint8Array {
    const decoded = atob(base64);
    const bytes = new Uint8Array(decoded.length);
    for (let index = 0; index < decoded.length; index += 1) {
        bytes[index] = decoded.charCodeAt(index);
    }
    return bytes;
}

function s3PageWasmEncryption(sync: NonNullable<DatabaseOpts["sync"]>) {
    const keyBytes = decodeBase64(sync.remoteEncryption.key);
    if (keyBytes.length !== 32) {
        throw new Error(
            "sync.remoteEncryption.key must be a base64-encoded 32-byte key for sync.backend = 's3_page_v1'",
        );
    }
    return {
        cipher: EncryptionCipher.Aegis256,
        hexkey: bytesToHex(keyBytes),
    };
}

function decryptS3PageManifestBytes(sync: NonNullable<DatabaseOpts["sync"]>, aad: string, bytes: Uint8Array): Uint8Array {
    return decryptS3PageMetadata(
        bytes,
        EncryptionCipher.Aegis256,
        s3PageWasmEncryption(sync).hexkey,
        aad,
    ) as Uint8Array;
}

function randomClientId(prefix?: string): string {
    const label = prefix ?? "browser";
    if (typeof crypto.randomUUID === "function") {
        return `${label}-${crypto.randomUUID()}`;
    }
    const bytes = new Uint8Array(16);
    crypto.getRandomValues(bytes);
    return `${label}-${bytesToHex(bytes)}`;
}

function s3PageReadonlyError(category: string): Error {
    return new Error(
        `sync.backend = 's3_page_v1' is read-only in the wasm runtime until commit-time S3 publication is wired; attempted ${category} statement`,
    );
}

async function readOpfsFile(path: string): Promise<Uint8Array | null> {
    try {
        const root = await navigator.storage.getDirectory();
        const handle = await root.getFileHandle(path);
        const file = await handle.getFile();
        return new Uint8Array(await file.arrayBuffer());
    } catch (error) {
        if ((error as DOMException).name === "NotFoundError") {
            return null;
        }
        throw error;
    }
}

async function deleteOpfsFile(path: string): Promise<void> {
    try {
        const root = await navigator.storage.getDirectory();
        await root.removeEntry(path);
    } catch (error) {
        if ((error as DOMException).name !== "NotFoundError") {
            throw error;
        }
    }
}

class Database extends DatabasePromise {
    #runner: Runner;
    #engine: any;
    #io: ProtocolIo;
    #guards: SyncEngineGuards;
    #worker: Worker | null;
    #remoteWriter: RemoteWriter | null = null;
    #db: any;
    #opts: DatabaseOpts;
    #s3PageSync: DatabaseOpts["sync"] | null = null;
    constructor(opts: DatabaseOpts) {
        if (opts.sync?.backend === "s3_page_v1") {
            if (opts.path === ":memory:") {
                throw new Error(
                    "sync.backend = 's3_page_v1' requires an OPFS-backed path in the wasm runtime",
                );
            }
            const db = new NativeDatabase(opts.path, {
                tracing: opts.tracing,
                encryption: s3PageWasmEncryption(opts.sync),
            }) as any;
            super(
                db,
                () => ioNotifier.waitForCompletion(),
            );
            this.#db = db;
            this.#opts = opts;
            this.#engine = null;
            this.#io = BrowserIO;
            this.#guards = new SyncEngineGuards();
            this.#worker = null;
            this.#s3PageSync = opts.sync;
            return;
        }
        if (opts.url == null) {
            const db = new NativeDatabase(opts.path, { tracing: opts.tracing }) as any;
            super(
                db,
                () => ioNotifier.waitForCompletion(),
            );
            this.#db = db;
            this.#opts = opts;
            this.#engine = null;
            this.#io = BrowserIO;
            this.#guards = new SyncEngineGuards();
            this.#worker = null;
            return;
        }

        let partialSyncOpts = undefined;
        if (opts.partialSyncExperimental != null) {
            switch (opts.partialSyncExperimental.bootstrapStrategy.kind) {
                case "prefix":
                    partialSyncOpts = {
                        bootstrapStrategy: { type: "Prefix", length: opts.partialSyncExperimental.bootstrapStrategy.length },
                        segmentSize: opts.partialSyncExperimental.segmentSize,
                        prefetch: opts.partialSyncExperimental.prefetch,
                    };
                    break;
                case "query":
                    partialSyncOpts = {
                        bootstrapStrategy: { type: "Query", query: opts.partialSyncExperimental.bootstrapStrategy.query },
                        segmentSize: opts.partialSyncExperimental.segmentSize,
                        prefetch: opts.partialSyncExperimental.prefetch,
                    };
                    break;
            }
        }
        const engine = new SyncEngine({
            path: opts.path,
            clientName: opts.clientName,
            useTransform: opts.transform != null,
            protocolVersion: SyncEngineProtocolVersion.V1,
            longPollTimeoutMs: opts.longPollTimeoutMs,
            tracing: opts.tracing,
            bootstrapIfEmpty: typeof opts.url != "function" || opts.url() != null,
            remoteEncryption: opts.remoteEncryption?.cipher,
            partialSyncOpts: partialSyncOpts
        });

        let headers: { [K: string]: string } | (() => Promise<{ [K: string]: string }>);
        if (typeof opts.authToken == "function") {
            const authToken = opts.authToken;
            headers = async () => ({
                ...(opts.authToken != null && { "Authorization": `Bearer ${await authToken()}` }),
                ...(opts.remoteEncryption != null && {
                    "x-turso-encryption-key": opts.remoteEncryption.key,
                    "x-turso-encryption-cipher": opts.remoteEncryption.cipher,
                })
            });
        } else {
            const authToken = opts.authToken;
            headers = {
                ...(opts.authToken != null && { "Authorization": `Bearer ${authToken}` }),
                ...(opts.remoteEncryption != null && {
                    "x-turso-encryption-key": opts.remoteEncryption.key,
                    "x-turso-encryption-cipher": opts.remoteEncryption.cipher,
                })
            };
        }
        const runOpts = {
            url: opts.url,
            headers: headers,
            preemptionMs: 1,
            transform: opts.transform,
        };
        const db = engine.db() as unknown as any;
        const memory = db.memory;
        const io = memory ? memoryIO() : BrowserIO;
        const run = runner(runOpts, io, engine);
        super(db, () => run.wait());

        this.#db = db;
        this.#opts = opts;
        this.#runner = run;
        this.#engine = engine;
        this.#io = io;
        this.#guards = new SyncEngineGuards();
        this.#worker = null;

        // Initialize remote writer if remoteWrites is enabled
        if (opts.remoteWritesExperimental && opts.url) {
            const url = resolveUrl(opts.url);
            this.#remoteWriter = new RemoteWriter({
                url,
                authToken: opts.authToken,
                remoteEncryptionKey: opts.remoteEncryption?.key,
            });
        }
    }
    /**
     * connect database and initialize it in case of clean start
     */
    override async connect() {
        if (this.connected) {
            return;
        } else if (this.#s3PageSync != null) {
            if (!this.memory) {
                this.#worker = await init();
                await this.#registerS3PageFiles();
            }
            await super.connect();
            await this.#loadS3PageMetadata();
            await this.#recoverS3PageSyncIfNeeded();
        } else if (this.#engine == null) {
            if (!this.memory) {
                const worker = await init();
                await Promise.all([
                    registerFileAtWorker(worker, this.name),
                    registerFileAtWorker(worker, `${this.name}-wal`)
                ]);
                this.#worker = worker;
            }
            await super.connect();
        } else {
            if (!this.memory) {
                this.#worker = await init();
                await Promise.all([
                    registerFileAtWorker(this.#worker, this.name),
                    registerFileAtWorker(this.#worker, `${this.name}-wal`),
                    registerFileAtWorker(this.#worker, `${this.name}-wal-revert`),
                    registerFileAtWorker(this.#worker, `${this.name}-info`),
                    registerFileAtWorker(this.#worker, `${this.name}-changes`),
                ]);
            }
            await run(this.#runner, this.#engine.connect());
        }
        this.connected = true;
    }
    /**
     * pull new changes from the remote database
     * if {@link DatabaseOpts.longPollTimeoutMs} is set - then server will hold the connection open until either new changes will appear in the database or timeout occurs.
     * @returns true if new changes were pulled from the remote
     */
    async pull() {
        if (this.#s3PageSync != null) {
            await this.connect();
            return await this.#pullS3PageSnapshot();
        }
        if (this.#engine == null) {
            throw new Error("sync is disabled as database was opened without sync support")
        }
        const changes = await this.#guards.wait(async () => await run(this.#runner, this.#engine.wait()));
        if (changes.empty()) {
            return false;
        }
        await this.#guards.apply(async () => await run(this.#runner, this.#engine.apply(changes)));
        return true;
    }

    async sync() {
        return await this.pull();
    }
    /**
     * push new local changes to the remote database
     * if {@link DatabaseOpts.transform} is set - then provided callback will be called for every mutation before sending it to the remote
     */
    async push() {
        if (this.#s3PageSync != null) {
            throw new Error(
                "push() is not supported for sync.backend = 's3_page_v1' in the wasm runtime",
            );
        }
        if (this.#engine == null) {
            throw new Error("sync is disabled as database was opened without sync support")
        }
        await this.#guards.push(async () => await run(this.#runner, this.#engine.push()));
    }
    /**
     * checkpoint WAL for local database
     */
    async checkpoint() {
        if (this.#s3PageSync != null) {
            throw new Error(
                "checkpoint() is not wired for sync.backend = 's3_page_v1' in the wasm runtime",
            );
        }
        if (this.#engine == null) {
            throw new Error("sync is disabled as database was opened without sync support")
        }
        await this.#guards.checkpoint(async () => await run(this.#runner, this.#engine.checkpoint()));
    }
    /**
     * @returns statistic of current local database
     */
    async stats(): Promise<DatabaseStats> {
        if (this.#s3PageSync != null) {
            throw new Error(
                "stats() is not wired for sync.backend = 's3_page_v1' in the wasm runtime",
            );
        }
        if (this.#engine == null) {
            throw new Error("sync is disabled as database was opened without sync support")
        }
        return (await run(this.#runner, this.#engine.stats()));
    }

    /**
     * Executes the given SQL string.
     * When remoteWrites is enabled, write statements are sent to the remote server.
     */
    override async exec(sql: string) {
        if (this.#s3PageSync != null) {
            const category = this.#db.classifySql(sql);
            if (category !== "read") {
                throw s3PageReadonlyError(category);
            }
            return await super.exec(sql);
        }
        if (!this.#remoteWriter) return super.exec(sql);
        const category = this.#db.classifySql(sql);

        if (this.#remoteWriter.isInTransaction) {
            const { shouldPull } = await this.#remoteWriter.execRemote(sql, category);
            if (shouldPull) await this.pull();
            return;
        }

        if (category === "read") return super.exec(sql);

        const { shouldPull } = await this.#remoteWriter.execRemote(sql, category);
        if (shouldPull) await this.pull();
    }

    /**
     * Prepares a SQL statement for execution.
     * When remoteWrites is enabled, returns a wrapper that routes writes to remote.
     */
    override prepare(sql: string) {
        if (this.#s3PageSync != null) {
            const category = this.#db.classifySql(sql);
            if (category !== "read") {
                throw s3PageReadonlyError(category);
            }
        }
        const localStmt = super.prepare(sql);

        if (this.#s3PageSync != null) {
            return localStmt;
        }

        if (!this.#remoteWriter) {
            return localStmt;
        }

        const category = this.#db.classifySql(sql);
        const isReadonly = category === "read";
        return new RemoteWriteStatement(
            localStmt,
            sql,
            isReadonly,
            this.#remoteWriter,
            () => this.pull(),
        ) as any;
    }

    /**
     * Returns a function that executes the given function in a transaction.
     * When remoteWrites is enabled, the entire transaction goes to remote.
     */
    override transaction(fn: (...any) => Promise<any>) {
        if (typeof fn !== "function")
            throw new TypeError("Expected first argument to be a function");

        if (this.#s3PageSync != null) {
            throw new Error(
                "transaction() is not supported for sync.backend = 's3_page_v1' in the wasm runtime while writes are disabled",
            );
        }

        if (!this.#remoteWriter) {
            return super.transaction(fn);
        }

        const db = this;
        const remoteWriter = this.#remoteWriter;
        const wrapTxn = (mode: string) => {
            return async (...bindParameters: any[]) => {
                await remoteWriter.beginTransaction(mode);
                try {
                    const result = await fn(...bindParameters);
                    await remoteWriter.commitTransaction();
                    await db.pull();
                    return result;
                } catch (err) {
                    await remoteWriter.rollbackTransaction();
                    throw err;
                }
            };
        };
        const properties = {
            default: { value: wrapTxn("") },
            deferred: { value: wrapTxn("DEFERRED") },
            immediate: { value: wrapTxn("IMMEDIATE") },
            exclusive: { value: wrapTxn("EXCLUSIVE") },
            database: { value: this, enumerable: true },
        };
        Object.defineProperties(properties.default.value, properties);
        Object.defineProperties(properties.deferred.value, properties);
        Object.defineProperties(properties.immediate.value, properties);
        Object.defineProperties(properties.exclusive.value, properties);
        return properties.default.value;
    }

    /**
     * close the database and relevant files
     */
    async close() {
        if (this.#remoteWriter) {
            await this.#remoteWriter.close();
        }
        if (this.#s3PageSync != null) {
            await super.close();
            this.connected = false;
            await this.#unregisterS3PageFiles();
            return;
        }
        if (this.#engine != null) {
            if (this.name != null && this.#worker != null) {
                await Promise.all([
                    unregisterFileAtWorker(this.#worker, this.name),
                    unregisterFileAtWorker(this.#worker, `${this.name}-wal`),
                    unregisterFileAtWorker(this.#worker, `${this.name}-wal-revert`),
                    unregisterFileAtWorker(this.#worker, `${this.name}-info`),
                    unregisterFileAtWorker(this.#worker, `${this.name}-changes`),
                ]);
            }
            this.#engine.close();
        }
        await super.close();
    }

    async #loadS3PageMetadata(): Promise<S3PageLocalMetadata> {
        const raw = await BrowserIO.read(s3PageInfoPath(this.#opts.path));
        if (raw != null) {
            return decodeUtf8Json(raw instanceof Uint8Array ? raw : new Uint8Array(raw));
        }

        const sync = this.#s3PageSync;
        if (sync == null) {
            throw new Error("S3 page sync is not configured");
        }
        const databaseId = `s3_page_v1:${new URL(sync.endpoint).toString()}/${sync.bucket}/${sync.prefix}`;
        const metadata: S3PageLocalMetadata = {
            version: 1,
            database_id: databaseId,
            client_id: randomClientId(this.#opts.clientName),
            head_commit_id: null,
            sequence_no: 0,
            head_etag: null,
            last_pulled_at: null,
            pending_publish_commit_id: null,
            selector_id: null,
            selector_hash: null,
            selector_version: null,
        };
        await this.#saveS3PageMetadata(metadata);
        return metadata;
    }

    async #saveS3PageMetadata(metadata: S3PageLocalMetadata): Promise<void> {
        await BrowserIO.write(
            s3PageInfoPath(this.#opts.path),
            new TextEncoder().encode(JSON.stringify(metadata)),
        );
    }

    #s3PageClient(): S3PageClient {
        if (this.#s3PageSync == null) {
            throw new Error("S3 page sync is not configured");
        }
        return new S3PageClient(this.#s3PageSync);
    }

    async #pullS3PageSnapshot(): Promise<boolean> {
        const client = this.#s3PageClient();
        const localMeta = await this.#loadS3PageMetadata();
        const headObject = await client.getHeadRef();
        if (headObject == null) {
            return false;
        }

        const head = decodeUtf8Json<S3PageHeadManifest>(
            decryptS3PageManifestBytes(this.#s3PageSync!, "refs/head", headObject.body),
        );
        if (
            localMeta.head_commit_id === head.head_commit_id &&
            localMeta.sequence_no === head.sequence_no &&
            localMeta.head_etag === headObject.etag
        ) {
            return false;
        }

        const commits = await this.#loadMissingS3PageCommits(
            client,
            localMeta.head_commit_id ?? null,
            head,
        );
        if (commits.length === 0) {
            localMeta.head_commit_id = head.head_commit_id;
            localMeta.sequence_no = head.sequence_no;
            localMeta.head_etag = headObject.etag;
            localMeta.last_pulled_at = currentUnixSeconds();
            localMeta.pending_publish_commit_id = null;
            await this.#saveS3PageMetadata(localMeta);
            return true;
        }

        await super.exec("PRAGMA wal_checkpoint(TRUNCATE)");
        await this.#closeS3PageLocalConnection();
        try {
            await this.#applyS3PageCommits(client, commits);
            localMeta.database_id = head.database_id;
            localMeta.head_commit_id = head.head_commit_id;
            localMeta.sequence_no = head.sequence_no;
            localMeta.head_etag = headObject.etag;
            localMeta.last_pulled_at = currentUnixSeconds();
            localMeta.pending_publish_commit_id = null;
            await this.#saveS3PageMetadata(localMeta);
        } catch (error) {
            await this.#refreshS3PageLocalConnection();
            throw error;
        }
        await this.#refreshS3PageLocalConnection();
        return true;
    }

    async #recoverS3PageSyncIfNeeded(): Promise<boolean> {
        const localMeta = await this.#loadS3PageMetadata();
        const pendingCommitId = localMeta.pending_publish_commit_id ?? null;
        if (pendingCommitId == null) {
            return false;
        }

        const client = this.#s3PageClient();
        const headObject = await client.getHeadRef();
        if (headObject == null) {
            localMeta.pending_publish_commit_id = null;
            await this.#saveS3PageMetadata(localMeta);
            return false;
        }

        const head = decodeUtf8Json<S3PageHeadManifest>(
            decryptS3PageManifestBytes(this.#s3PageSync!, "refs/head", headObject.body),
        );
        if (
            localMeta.head_commit_id === pendingCommitId &&
            head.head_commit_id === pendingCommitId &&
            localMeta.sequence_no === head.sequence_no
        ) {
            localMeta.head_etag = headObject.etag;
            localMeta.pending_publish_commit_id = null;
            await this.#saveS3PageMetadata(localMeta);
            return false;
        }

        const missingCommits = await this.#loadMissingS3PageCommits(
            client,
            localMeta.head_commit_id ?? null,
            head,
        );
        if (missingCommits.some((commit) => commit.commit_id === pendingCommitId)) {
            return await this.#pullS3PageSnapshot();
        }

        localMeta.pending_publish_commit_id = null;
        await this.#saveS3PageMetadata(localMeta);
        return false;
    }

    async #loadMissingS3PageCommits(
        client: S3PageClient,
        localHeadCommitId: string | null,
        head: S3PageHeadManifest,
    ): Promise<S3PageCommitManifest[]> {
        const commits: S3PageCommitManifest[] = [];
        const seenCommitIds = new Set<string>();
        let nextCommitId: string | null = head.head_commit_id;

        while (nextCommitId != null && nextCommitId !== localHeadCommitId) {
            if (seenCommitIds.has(nextCommitId)) {
                throw new Error(`remote commit history contains a cycle at ${nextCommitId}`);
            }
            seenCommitIds.add(nextCommitId);

            const commitObject = await client.getObject(`commits/${nextCommitId}`);
            if (commitObject == null) {
                throw new Error(`missing remote commit object for ${nextCommitId}`);
            }
            const commit = decodeUtf8Json<S3PageCommitManifest>(
                decryptS3PageManifestBytes(
                    this.#s3PageSync!,
                    `commits/${nextCommitId}`,
                    commitObject.body,
                ),
            );
            commits.push(commit);
            nextCommitId = commit.parent_commit_id ?? null;
        }

        if (localHeadCommitId != null && nextCommitId !== localHeadCommitId) {
            throw new Error(
                `local head ${localHeadCommitId} is not an ancestor of remote head ${head.head_commit_id}`,
            );
        }

        commits.reverse();
        return commits;
    }

    async #applyS3PageCommits(
        client: S3PageClient,
        commits: S3PageCommitManifest[],
    ): Promise<void> {
        let pageSize = await this.#readLocalS3PageSize();
        const root = await navigator.storage.getDirectory();
        const handle = await root.getFileHandle(this.#opts.path, { create: true });
        const writable = await handle.createWritable({ keepExistingData: true });
        try {
            for (const commit of commits) {
                for (const segmentRef of commit.segments) {
                    const segmentObject = await client.getObject(`segments/${segmentRef.segment_id}`);
                    if (segmentObject == null) {
                        throw new Error(`missing remote segment object for ${segmentRef.segment_id}`);
                    }
                    const pages = decodeS3PageSegment(segmentObject.body);
                    for (const page of pages) {
                        if (pageSize == null && page.pageId === 1) {
                            pageSize = parseSqlitePageSize(page.bytes);
                        }
                        if (pageSize == null) {
                            throw new Error(
                                `unable to determine database page size while applying ${commit.commit_id}`,
                            );
                        }
                        if (page.bytes.byteLength !== pageSize) {
                            throw new Error(
                                `remote page ${page.pageId} has unexpected size ${page.bytes.byteLength}, expected ${pageSize}`,
                            );
                        }
                        const pageOffset = (page.pageId - 1) * pageSize;
                        const pageCopy = page.bytes.slice();
                        await writable.write({ type: "write", position: pageOffset, data: pageCopy.buffer });
                    }
                }
                if (pageSize == null) {
                    throw new Error(
                        `unable to determine database page size after applying ${commit.commit_id}`,
                    );
                }
                await writable.truncate(commit.page_count * pageSize);
            }
        } finally {
            await writable.close();
        }
        await this.#resetS3PageSideFiles();
    }

    async #readLocalS3PageSize(): Promise<number | null> {
        const bytes = await readOpfsFile(this.#opts.path);
        if (bytes == null) {
            return null;
        }
        return parseSqlitePageSize(bytes);
    }

    async #resetS3PageSideFiles(): Promise<void> {
        for (const suffix of ["-wal", "-log", "-shm"]) {
            await deleteOpfsFile(`${this.#opts.path}${suffix}`);
        }
    }

    async #registerS3PageFiles(): Promise<void> {
        if (this.#worker == null) {
            return;
        }
        const path = this.#opts.path;
        await Promise.all([
            registerFileAtWorker(this.#worker, path),
            registerFileAtWorker(this.#worker, `${path}-wal`),
        ]);
    }

    async #unregisterS3PageFiles(): Promise<void> {
        if (this.#worker == null) {
            return;
        }
        const path = this.#opts.path;
        await Promise.all([
            unregisterFileAtWorker(this.#worker, path),
            unregisterFileAtWorker(this.#worker, `${path}-wal`),
        ]);
    }

    async #closeS3PageLocalConnection() {
        await super.close();
        this.connected = false;
        await this.#unregisterS3PageFiles();
    }

    async #refreshS3PageLocalConnection() {
        const db = new NativeDatabase(this.#opts.path, {
            tracing: this.#opts.tracing,
            encryption: this.#s3PageSync == null ? undefined : s3PageWasmEncryption(this.#s3PageSync),
        }) as any;
        this.#db = db;
        (this as any).db = db;
        if (!this.memory) {
            if (this.#worker == null) {
                this.#worker = await init();
            }
            await this.#registerS3PageFiles();
        }
        await super.connect();
        this.connected = true;
    }
}

/**
 * Creates a new database connection asynchronously.
 *
 * @param {Object} opts - Options for database behavior.
 * @returns {Promise<Database>} - A promise that resolves to a Database instance.
 */
async function connect(opts: DatabaseOpts): Promise<Database> {
    const db = new Database(opts);
    await db.connect();
    return db;
}

export { connect, Database }
export type { DatabaseOpts, EncryptionOpts, DatabaseRowMutation, DatabaseRowStatement, DatabaseRowTransformResult }
