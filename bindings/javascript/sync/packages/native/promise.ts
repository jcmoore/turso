import { DatabasePromise } from "@tursodatabase/database-common"
import { ProtocolIo, run, DatabaseOpts, EncryptionOpts, RunOpts, DatabaseRowMutation, DatabaseRowStatement, DatabaseRowTransformResult, DatabaseStats, SyncEngineGuards, Runner, runner, RemoteWriter, RemoteWriteStatement, S3PageClient, SyncConflictError } from "@tursodatabase/sync-common";
import { SyncEngine, SyncEngineProtocolVersion, Database as NativeDatabase, EncryptionCipher, decryptS3PageMetadata } from "#index";
import { promises } from "node:fs";
import { Buffer } from "node:buffer";

const S3_PAGE_INFO_SUFFIX = "-s3-page-info";
const S3_PAGE_SEGMENT_MAGIC = "TS3P";
const SQLITE_PAGE_SIZE_OFFSET = 16;
const SQLITE_PAGE_SIZE_LENGTH = 2;

let NodeIO: ProtocolIo = {
    async read(path: string): Promise<Buffer | Uint8Array | null> {
        try {
            return await promises.readFile(path);
        } catch (error) {
            if (error.code === 'ENOENT') {
                return null;
            }
            throw error;
        }
    },
    async write(path: string, data: Buffer | Uint8Array): Promise<void> {
        const unix = Math.floor(Date.now() / 1000);
        const nonce = Math.floor(Math.random() * 1000000000);
        const tmp = `${path}.tmp.${unix}.${nonce}`;
        await promises.writeFile(tmp, new Uint8Array(data));
        try {
            await promises.rename(tmp, path);
        } catch (err) {
            await promises.unlink(tmp);
            throw err;
        }
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

function normalizeS3PageSyncError(error: unknown): never {
    if (isS3PageSyncConflictLike(error)) {
        throw new SyncConflictError(
            error instanceof Error ? error.message : "Sync conflict",
            error,
        );
    }
    throw error;
}

function isS3PageSyncConflictLike(error: unknown): boolean {
    if (error instanceof SyncConflictError) {
        return true;
    }
    if (!(error instanceof Error)) {
        return false;
    }

    const code = (error as any).code;
    const rawCode = (error as any).rawCode;
    const message = error.message.toLowerCase();
    return (
        code === "SYNC_CONFLICT" ||
        rawCode === "SYNC_CONFLICT" ||
        code === "SQLITE_BUSY_SNAPSHOT" ||
        rawCode === "SQLITE_BUSY_SNAPSHOT" ||
        message.includes("database snapshot is stale") ||
        message.includes("busy snapshot")
    );
}

type S3PageNativeDatabase = NativeDatabase & {
    enableS3PageSync(opts: {
        endpoint: string;
        bucket: string;
        prefix: string;
        region?: string;
        clientName?: string;
        metadataEncryptionHexkey: string;
        credentials: {
            accessKeyId: string;
            secretAccessKey: string;
            sessionToken?: string;
        };
    }): void;
    captureS3PageSyncState(): boolean;
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
    return JSON.parse(new TextDecoder().decode(bytes)) as T;
}

function decryptS3PageManifestBytes(sync: NonNullable<DatabaseOpts["sync"]>, aad: string, bytes: Uint8Array): Uint8Array {
    return decryptS3PageMetadata(
        Buffer.from(bytes),
        EncryptionCipher.Aegis256,
        s3PageNativeEncryption(sync).hexkey,
        aad,
    );
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
    const magic = Buffer.from(payload.subarray(0, 4)).toString("utf8");
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

function s3PageNativeEncryption(sync: NonNullable<DatabaseOpts["sync"]>) {
    const keyBytes = Buffer.from(sync.remoteEncryption.key, "base64");
    if (keyBytes.length !== 32) {
        throw new Error(
            "sync.remoteEncryption.key must be a base64-encoded 32-byte key for sync.backend = 's3_page_v1'",
        );
    }
    return {
        cipher: EncryptionCipher.Aegis256,
        hexkey: keyBytes.toString("hex"),
    };
}

class S3PageStatement {
    #stmt: any;
    #persist: (() => Promise<void>) | null;

    constructor(stmt: any, shouldPersist: boolean, persist: () => Promise<void>) {
        this.#stmt = stmt;
        this.#persist = shouldPersist ? persist : null;
    }

    raw(raw?: boolean) {
        this.#stmt.raw(raw);
        return this;
    }

    pluck(pluckMode?: boolean) {
        this.#stmt.pluck(pluckMode);
        return this;
    }

    safeIntegers(toggle?: boolean) {
        this.#stmt.safeIntegers(toggle);
        return this;
    }

    columns() {
        return this.#stmt.columns();
    }

    get source() {
        return this.#stmt.source;
    }

    get reader() {
        return this.#stmt.reader;
    }

    get database() {
        return this.#stmt.database;
    }

    bind(...bindParameters: any[]) {
        this.#stmt.bind(...bindParameters);
        return this;
    }

    async run(...bindParameters: any[]) {
        try {
            const result = await this.#stmt.run(...bindParameters);
            await this.#persistIfNeeded();
            return result;
        } catch (error) {
            normalizeS3PageSyncError(error);
        }
    }

    async get(...bindParameters: any[]) {
        try {
            const result = await this.#stmt.get(...bindParameters);
            await this.#persistIfNeeded();
            return result;
        } catch (error) {
            normalizeS3PageSyncError(error);
        }
    }

    async all(...bindParameters: any[]) {
        try {
            const result = await this.#stmt.all(...bindParameters);
            await this.#persistIfNeeded();
            return result;
        } catch (error) {
            normalizeS3PageSyncError(error);
        }
    }

    async *iterate(...bindParameters: any[]) {
        try {
            for await (const row of this.#stmt.iterate(...bindParameters)) {
                yield row;
            }
            await this.#persistIfNeeded();
        } catch (error) {
            normalizeS3PageSyncError(error);
        }
    }

    close() {
        return this.#stmt.close();
    }

    async #persistIfNeeded() {
        if (this.#persist == null) {
            return;
        }
        await this.#persist();
    }
}

class Database extends DatabasePromise {
    #engine: any;
    #guards: SyncEngineGuards;
    #runner: Runner;
    #remoteWriter: RemoteWriter | null = null;
    #db: any;
    #opts: DatabaseOpts;
    #s3PageSync: DatabaseOpts["sync"] | null = null;
    #s3PageSyncConfigured = false;
    #clientName: string | undefined;
    constructor(opts: DatabaseOpts) {
        const nativeDb = (nativeOpts: DatabaseOpts) =>
            new NativeDatabase(nativeOpts.path, {
                tracing: nativeOpts.tracing,
                encryption: nativeOpts.sync?.backend === "s3_page_v1"
                    ? s3PageNativeEncryption(nativeOpts.sync)
                    : undefined,
            }) as any;
        if (opts.sync?.backend === "s3_page_v1") {
            const db = nativeDb(opts);
            super(db);
            this.#db = db;
            this.#opts = opts;
            this.#engine = null;
            this.#s3PageSync = opts.sync;
            this.#clientName = opts.clientName;
            return;
        }
        if (opts.url == null) {
            const db = nativeDb(opts);
            super(db);
            this.#db = db;
            this.#opts = opts;
            this.#engine = null;
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
            remoteEncryptionCipher: opts.remoteEncryption?.cipher,
            remoteEncryptionKey: opts.remoteEncryption?.key,
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
        const io = memory ? memoryIO() : NodeIO;
        const run = runner(runOpts, io, engine);

        super(engine.db() as unknown as any, () => run.wait());

        this.#db = engine.db() as unknown as any;
        this.#opts = opts;
        this.#runner = run;
        this.#engine = engine;
        this.#guards = new SyncEngineGuards();

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
            await super.connect();
            await this.#configureS3PageSyncIfNeeded();
            await this.#recoverS3PageSyncIfNeeded();
        } else if (this.#engine == null) {
            await super.connect();
        } else {
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
            return;
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
                "checkpoint() is not wired for sync.backend = 's3_page_v1' yet",
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
                "stats() is not wired for sync.backend = 's3_page_v1' yet",
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
            try {
                await super.exec(sql);
                await this.#captureS3PageSyncState();
                return;
            } catch (error) {
                normalizeS3PageSyncError(error);
            }
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
        const localStmt = super.prepare(sql);

        if (this.#s3PageSync != null) {
            const category = this.#db.classifySql(sql);
            return new S3PageStatement(
                localStmt,
                category !== "read",
                async () => {
                    await this.#captureS3PageSyncState();
                },
            ) as any;
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
            const db = this;
            const wrapTxn = (mode: string) => {
                return async (...bindParameters: any[]) => {
                    await db.exec("BEGIN " + mode);
                    try {
                        const result = await fn(...bindParameters);
                        await db.exec("COMMIT");
                        return result;
                    } catch (err) {
                        await db.exec("ROLLBACK");
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

    async #configureS3PageSyncIfNeeded() {
        if (this.#s3PageSync == null || this.#s3PageSyncConfigured) {
            return;
        }
        const credentials = await this.#s3PageSync.credentials();
        (this.#db as S3PageNativeDatabase).enableS3PageSync({
            endpoint: this.#s3PageSync.endpoint,
            bucket: this.#s3PageSync.bucket,
            prefix: this.#s3PageSync.prefix,
            region: this.#s3PageSync.region,
            clientName: this.#clientName,
            metadataEncryptionHexkey: s3PageNativeEncryption(this.#s3PageSync).hexkey,
            credentials,
        });
        this.#s3PageSyncConfigured = true;
    }

    async #loadS3PageMetadata(): Promise<S3PageLocalMetadata> {
        const raw = await promises.readFile(s3PageInfoPath(this.#opts.path), "utf8");
        return JSON.parse(raw) as S3PageLocalMetadata;
    }

    async #saveS3PageMetadata(metadata: S3PageLocalMetadata): Promise<void> {
        await NodeIO.write(
            s3PageInfoPath(this.#opts.path),
            Buffer.from(JSON.stringify(metadata)),
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
            await this.#refreshS3PageNativeConnection();
            return true;
        }

        await super.exec("PRAGMA wal_checkpoint(TRUNCATE)");
        await this.#closeS3PageNativeConnection();
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
            await this.#refreshS3PageNativeConnection();
            throw error;
        }
        await this.#refreshS3PageNativeConnection();
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
        const dbHandle = await promises.open(this.#opts.path, "r+");
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
                        const pageOffset = BigInt(page.pageId - 1) * BigInt(pageSize);
                        await dbHandle.write(Buffer.from(page.bytes), 0, page.bytes.byteLength, Number(pageOffset));
                    }
                }
                if (pageSize == null) {
                    throw new Error(
                        `unable to determine database page size after applying ${commit.commit_id}`,
                    );
                }
                await dbHandle.truncate(commit.page_count * pageSize);
            }
        } finally {
            await dbHandle.close();
        }
        await this.#resetS3PageSideFiles();
    }

    async #readLocalS3PageSize(): Promise<number | null> {
        try {
            const pageHeader = await promises.readFile(this.#opts.path);
            return parseSqlitePageSize(pageHeader);
        } catch (error) {
            if ((error as NodeJS.ErrnoException).code === "ENOENT") {
                return null;
            }
            throw error;
        }
    }

    async #resetS3PageSideFiles(): Promise<void> {
        for (const suffix of ["-wal", "-log", "-shm"]) {
            try {
                await promises.unlink(`${this.#opts.path}${suffix}`);
            } catch (error) {
                if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
                    throw error;
                }
            }
        }
    }

    async #closeS3PageNativeConnection() {
        await super.close();
        this.connected = false;
        this.#s3PageSyncConfigured = false;
    }

    async #refreshS3PageNativeConnection() {
        const db = new NativeDatabase(this.#opts.path, {
            tracing: this.#opts.tracing,
            encryption: this.#s3PageSync == null ? undefined : s3PageNativeEncryption(this.#s3PageSync),
        }) as any;
        this.#db = db;
        (this as any).db = db;
        await super.connect();
        this.connected = true;
        await this.#configureS3PageSyncIfNeeded();
    }

    /**
     * close the database
     */
    override async close(): Promise<void> {
        if (this.#remoteWriter) {
            await this.#remoteWriter.close();
        }
        await super.close();
        if (this.#engine != null) {
            this.#engine.close();
        }
    }

    async #captureS3PageSyncState() {
        if (this.#s3PageSync == null || !this.#s3PageSyncConfigured) {
            return false;
        }
        return (this.#db as S3PageNativeDatabase).captureS3PageSyncState();
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
export { SyncConflictError }
export { isS3PageSyncConflictLike }
export type { DatabaseOpts, EncryptionOpts, DatabaseRowMutation, DatabaseRowStatement, DatabaseRowTransformResult }
