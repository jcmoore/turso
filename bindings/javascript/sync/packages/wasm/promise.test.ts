import { expect, test, afterAll } from 'vitest'
import { Database, connect, DatabaseRowMutation, DatabaseRowTransformResult } from './promise-default.js'
import { registerFileAtWorker, unregisterFileAtWorker, ioNotifier } from '@tursodatabase/database-wasm-common'
import { DatabasePromise } from '@tursodatabase/database-common'
import { S3PageClient } from '@tursodatabase/sync-common'
import { Database as NativeDatabase, EncryptionCipher, MainWorker, encryptS3PageMetadata, initThreadPool } from './index-default.js'

afterAll(() => {
    MainWorker?.terminate();
})

const localeCompare = (a, b) => a.x.localeCompare(b.x);
const intCompare = (a, b) => a.x - b.x;
const ENCRYPTION_KEY = 'BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc=';
const S3_PAGE_SEGMENT_MAGIC = 'TS3P';

function bytesToHex(bytes: Uint8Array): string {
    return Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('');
}

function decodeBase64(base64: string): Uint8Array {
    const decoded = atob(base64);
    const bytes = new Uint8Array(decoded.length);
    for (let index = 0; index < decoded.length; index += 1) {
        bytes[index] = decoded.charCodeAt(index);
    }
    return bytes;
}

const ENCRYPTION_HEXKEY = bytesToHex(decodeBase64(ENCRYPTION_KEY));

function encodeSegment(pages: Uint8Array[]): Uint8Array {
    const totalBytes = pages.reduce((size, page) => size + 8 + page.byteLength, 12);
    const payload = new Uint8Array(totalBytes);
    const view = new DataView(payload.buffer);
    let offset = 0;
    payload.set(new TextEncoder().encode(S3_PAGE_SEGMENT_MAGIC), offset);
    offset += 4;
    view.setUint32(offset, 1, false);
    offset += 4;
    view.setUint32(offset, pages.length, false);
    offset += 4;
    for (let index = 0; index < pages.length; index += 1) {
        view.setUint32(offset, index + 1, false);
        offset += 4;
        view.setUint32(offset, pages[index].byteLength, false);
        offset += 4;
        payload.set(pages[index], offset);
        offset += pages[index].byteLength;
    }
    return payload;
}

async function readOpfsFile(path: string): Promise<Uint8Array> {
    const root = await navigator.storage.getDirectory();
    const handle = await root.getFileHandle(path);
    const file = await handle.getFile();
    return new Uint8Array(await file.arrayBuffer());
}

async function deleteOpfsFile(path: string): Promise<void> {
    try {
        const root = await navigator.storage.getDirectory();
        await root.removeEntry(path);
    } catch (error) {
        if ((error as DOMException).name !== 'NotFoundError') {
            throw error;
        }
    }
}

function pageSizeFromDatabaseBytes(bytes: Uint8Array): number {
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const value = view.getUint16(16, false);
    return value === 1 ? 65536 : value;
}

async function openEncryptedLocalDb(path: string): Promise<DatabasePromise> {
    await initThreadPool();
    if (MainWorker == null) {
        throw new Error('panic: MainWorker is not initialized');
    }
    await Promise.all([
        registerFileAtWorker(MainWorker, path),
        registerFileAtWorker(MainWorker, `${path}-wal`),
    ]);
    const db = new DatabasePromise(
        new NativeDatabase(path, {
            encryption: {
                cipher: EncryptionCipher.Aegis256,
                hexkey: ENCRYPTION_HEXKEY,
            },
        }) as any,
        () => ioNotifier.waitForCompletion(),
    );
    await db.connect();
    return db;
}

async function closeEncryptedLocalDb(path: string, db: DatabasePromise): Promise<void> {
    await db.close();
    if (MainWorker != null) {
        await Promise.all([
            unregisterFileAtWorker(MainWorker, path),
            unregisterFileAtWorker(MainWorker, `${path}-wal`),
        ]);
    }
}

async function snapshotDatabasePages(path: string): Promise<Uint8Array[]> {
    const bytes = await readOpfsFile(path);
    const pageSize = pageSizeFromDatabaseBytes(bytes);
    const pages: Uint8Array[] = [];
    for (let offset = 0; offset < bytes.byteLength; offset += pageSize) {
        pages.push(bytes.slice(offset, offset + pageSize));
    }
    return pages;
}

async function createSeedSnapshot(path: string, values: number[]): Promise<Uint8Array[]> {
    const db = await openEncryptedLocalDb(path);
    try {
        await db.exec("PRAGMA journal_mode = 'mvcc'");
        await db.exec('DROP TABLE IF EXISTS rows');
        await db.exec('CREATE TABLE rows(value INTEGER PRIMARY KEY)');
        if (values.length > 0) {
            await db.exec(`INSERT INTO rows VALUES ${values.map((value) => `(${value})`).join(', ')}`);
        }
        await db.exec('PRAGMA wal_checkpoint(TRUNCATE)');
    } finally {
        await closeEncryptedLocalDb(path, db);
    }
    return await snapshotDatabasePages(path);
}

async function publishRemoteSnapshot(opts: {
    client: S3PageClient;
    endpoint: string;
    bucket: string;
    prefix: string;
    commitId: string;
    parentCommitId: string | null;
    sequenceNo: number;
    pages: Uint8Array[];
    headEtag: string | null;
}): Promise<string> {
    const databaseId = `s3_page_v1:${new URL(opts.endpoint).toString()}/${opts.bucket}/${opts.prefix}`;
    const segmentId = `${opts.commitId}-segment-0`;
    await opts.client.putSegment(segmentId, encodeSegment(opts.pages));
    await opts.client.putCommit(
        opts.commitId,
        encryptS3PageMetadata(
            new TextEncoder().encode(
                JSON.stringify({
                    version: 1,
                    database_id: databaseId,
                    commit_id: opts.commitId,
                    parent_commit_id: opts.parentCommitId,
                    sequence_no: opts.sequenceNo,
                    page_count: opts.pages.length,
                    client_id: 'seed',
                    transaction_id: opts.commitId,
                    segments: [
                        {
                            segment_id: segmentId,
                            page_ids: opts.pages.map((_, index) => index + 1),
                        },
                    ],
                    selector: {},
                }),
            ),
            EncryptionCipher.Aegis256,
            ENCRYPTION_HEXKEY,
            `commits/${opts.commitId}`,
        ),
    );
    const headBytes = encryptS3PageMetadata(
        new TextEncoder().encode(
            JSON.stringify({
                version: 1,
                database_id: databaseId,
                head_commit_id: opts.commitId,
                sequence_no: opts.sequenceNo,
            }),
        ),
        EncryptionCipher.Aegis256,
        ENCRYPTION_HEXKEY,
        'refs/head',
    );
    if (opts.headEtag == null) {
        const created = await opts.client.putObjectIfAbsent('refs/head', headBytes);
        return created.etag!;
    }
    const updated = await opts.client.casHead(headBytes, opts.headEtag);
    return updated.etag!;
}

function createMockS3Fetch() {
    const buckets = new Set<string>();
    const objects = new Map<string, { body: Uint8Array; etag: string; contentType: string | null }>();
    let etagCounter = 0;

    const nextEtag = () => `etag-${++etagCounter}`;
    const objectKeyFor = (url: URL) => url.pathname.replace(/^\/+/, '');

    const fetchImpl: typeof fetch = async (input, init) => {
        const url = new URL(typeof input === 'string' ? input : input instanceof URL ? input.href : input.url);
        const method = (init?.method ?? 'GET').toUpperCase();
        const pathname = url.pathname.replace(/^\/+/, '');
        const headers = new Headers(init?.headers ?? {});
        const pathParts = pathname.split('/').filter(Boolean);

        if (method === 'PUT' && pathParts.length === 1) {
            const bucket = pathParts[0];
            if (buckets.has(bucket)) {
                return new Response(null, { status: 409 });
            }
            buckets.add(bucket);
            return new Response(null, { status: 200 });
        }

        const key = objectKeyFor(url);
        const current = objects.get(key);
        if (method === 'HEAD') {
            if (current == null) {
                return new Response(null, { status: 404 });
            }
            return new Response(null, {
                status: 200,
                headers: current.etag == null ? undefined : { etag: `"${current.etag}"` },
            });
        }
        if (method === 'GET') {
            if (current == null) {
                return new Response(null, { status: 404 });
            }
            const bodyCopy = current.body.slice();
            return new Response(bodyCopy.buffer as ArrayBuffer, {
                status: 200,
                headers: {
                    etag: `"${current.etag}"`,
                    ...(current.contentType == null ? {} : { 'content-type': current.contentType }),
                },
            });
        }
        if (method === 'PUT') {
            const ifNoneMatch = headers.get('if-none-match');
            const ifMatch = headers.get('if-match')?.replace(/^"|"$/g, '') ?? null;
            if (ifNoneMatch === '*' && current != null) {
                return new Response(null, { status: 412 });
            }
            if (ifMatch != null && current?.etag !== ifMatch) {
                return new Response(null, { status: 412 });
            }
            const body = init?.body == null ? new Uint8Array() : new Uint8Array(await new Response(init.body).arrayBuffer());
            const etag = nextEtag();
            objects.set(key, {
                body,
                etag,
                contentType: headers.get('content-type'),
            });
            return new Response(null, {
                status: 200,
                headers: { etag: `"${etag}"` },
            });
        }

        return new Response(null, { status: 405 });
    };

    return { fetchImpl };
}

test('open non-sync db', async () => {
    const db = await connect({ path: 'local.db' });
    await db.exec("CREATE TABLE t(x)");
    await db.exec("INSERT INTO t VALUES (1), (2), (3)");
    expect(await (await db.prepare("SELECT * FROM t").all())).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }])
})

test('s3-page wasm pull bootstraps and advances a pinned read replica', async () => {
    const originalFetch = globalThis.fetch;
    const { fetchImpl } = createMockS3Fetch();
    globalThis.fetch = fetchImpl;

    const endpoint = 'https://mock-s3.invalid';
    const bucket = `wasm-${Date.now()}`;
    const prefix = 'db/main';
    const sync = {
        backend: 's3_page_v1' as const,
        endpoint,
        bucket,
        prefix,
        credentials: async () => ({
            accessKeyId: 'key',
            secretAccessKey: 'secret',
        }),
        remoteEncryption: {
            key: ENCRYPTION_KEY,
            cipher: 'aegis256' as const,
        },
    };

    const client = new S3PageClient(sync);
    const writerOnePath = `s3-page-wasm-writer-${Date.now()}-1.db`;
    const writerTwoPath = `s3-page-wasm-writer-${Date.now()}-2.db`;
    const replicaPath = `s3-page-wasm-replica-${Date.now()}.db`;
    let db: Database | null = null;

    try {
        await client.createBucket();

        let headEtag = await publishRemoteSnapshot({
            client,
            endpoint,
            bucket,
            prefix,
            commitId: 'seed-1',
            parentCommitId: null,
            sequenceNo: 1,
            pages: await createSeedSnapshot(writerOnePath, [1, 2, 3]),
            headEtag: null,
        });

        db = await connect({
            path: replicaPath,
            sync,
        });

        expect(await db.pull()).toBe(true);
        expect(await db.prepare('SELECT value as x FROM rows ORDER BY value').all()).toEqual([
            { x: 1 },
            { x: 2 },
            { x: 3 },
        ]);

        headEtag = await publishRemoteSnapshot({
            client,
            endpoint,
            bucket,
            prefix,
            commitId: 'seed-2',
            parentCommitId: 'seed-1',
            sequenceNo: 2,
            pages: await createSeedSnapshot(writerTwoPath, [1, 2, 3, 4]),
            headEtag,
        });

        expect(await db.prepare('SELECT value as x FROM rows ORDER BY value').all()).toEqual([
            { x: 1 },
            { x: 2 },
            { x: 3 },
        ]);
        expect(await db.pull()).toBe(true);
        expect(await db.prepare('SELECT value as x FROM rows ORDER BY value').all()).toEqual([
            { x: 1 },
            { x: 2 },
            { x: 3 },
            { x: 4 },
        ]);
        await expect(async () => await db.exec('INSERT INTO rows VALUES (5)')).rejects.toThrow(
            /read-only in the wasm runtime/,
        );
    } finally {
        if (db != null) {
            await db.close();
        }
        await Promise.all([
            deleteOpfsFile(replicaPath),
            deleteOpfsFile(`${replicaPath}-wal`),
            deleteOpfsFile(`${replicaPath}-s3-page-info`),
            deleteOpfsFile(writerOnePath),
            deleteOpfsFile(`${writerOnePath}-wal`),
            deleteOpfsFile(writerTwoPath),
            deleteOpfsFile(`${writerTwoPath}-wal`),
        ]);
        globalThis.fetch = originalFetch;
    }
})

test('checkpoint-and-actions', async () => {
    {
        const db = await connect({
            path: ':memory:',
            url: process.env.VITE_TURSO_DB_URL,
            longPollTimeoutMs: 100,
        });
        await db.exec("CREATE TABLE IF NOT EXISTS rows(key TEXT PRIMARY KEY, value INTEGER)");
        await db.exec("DELETE FROM rows");
        await db.exec("INSERT INTO rows VALUES ('key', 0)");
        await db.push();
        await db.close();
    }
    const db1 = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
    await db1.exec("PRAGMA busy_timeout=100");
    console.info('run_info', await db1.prepare("SELECT * FROM sqlite_master").all());
    const pull = async function (iterations: number) {
        for (let i = 0; i < iterations; i++) {
            console.info('pull', i);
            try {
                await db1.pull();
            }
            catch (e) { console.error('pull', e); }
            await new Promise(resolve => setTimeout(resolve, 0));
        }
    }
    const push = async function (iterations: number) {
        for (let i = 0; i < iterations; i++) {
            await new Promise(resolve => setTimeout(resolve, 10));
            console.info('push', i);
            try {
                if ((await db1.stats()).cdcOperations > 0) {
                    const start = performance.now();
                    await db1.push();
                    console.info('push', performance.now() - start);
                }
            }
            catch (e) { console.error('push', e); }
        }
    }
    const run = async function (iterations: number) {
        let rows = 0;
        for (let i = 0; i < iterations; i++) {
            console.info('run', i, rows);
            await db1.prepare("UPDATE rows SET value = value + 1 WHERE key = ?").run('key');
            rows += 1;
            const { cnt } = await db1.prepare("SELECT value as cnt FROM rows WHERE key = ?").get(['key']);
            expect(cnt).toBe(rows);
            await new Promise(resolve => setTimeout(resolve, 1));
        }
    }
    await Promise.all([pull(20), push(20), run(1000)]);
})

test('implicit connect', async () => {
    const db = new Database({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
    const defer = db.prepare("SELECT * FROM not_found");
    await expect(async () => await defer.all()).rejects.toThrowError(/no such table: not_found/);
    expect(() => db.prepare("SELECT * FROM not_found")).toThrowError(/no such table: not_found/);
    expect(await db.prepare("SELECT 1 as x").all()).toEqual([{ x: 1 }]);
})

test('simple-db', async () => {
    const db = new Database({ path: ':memory:' });
    expect(await db.prepare("SELECT 1 as x").all()).toEqual([{ x: 1 }])
    await db.exec("CREATE TABLE t(x)");
    await db.exec("INSERT INTO t VALUES (1), (2), (3)");
    expect(await db.prepare("SELECT * FROM t").all()).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }])
    await expect(async () => await db.pull()).rejects.toThrowError(/sync is disabled as database was opened without sync support/);
})

test('reconnect-db', async () => {
    {
        const db = await connect({ path: 'local.db', url: process.env.VITE_TURSO_DB_URL });
        const stmt = db.prepare("SELECT * FROM turso_cdc");
        expect(await stmt.all()).toEqual([])
        stmt.close();
    }
    {
        const db = await connect({ path: 'local.db', url: process.env.VITE_TURSO_DB_URL });
        const stmt = db.prepare("SELECT * FROM turso_cdc");
        expect(await stmt.all()).toEqual([])
        stmt.close();
    }
})

test('implicit connect', async () => {
    const db = new Database({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
    const defer = db.prepare("SELECT * FROM not_found");
    await expect(async () => await defer.all()).rejects.toThrowError(/no such table: not_found/);
    expect(() => db.prepare("SELECT * FROM not_found")).toThrowError(/no such table: not_found/);
    expect(await db.prepare("SELECT 1 as x").all()).toEqual([{ x: 1 }]);
})

test('defered sync', async () => {
    {
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
        await db.exec("CREATE TABLE IF NOT EXISTS t(x)");
        await db.exec("DELETE FROM t");
        await db.exec("INSERT INTO t VALUES (100)");
        await db.push();
        await db.close();
    }

    let url = null;
    const db = new Database({ path: ':memory:', url: () => url });
    await db.prepare("CREATE TABLE t(x)").run();
    await db.prepare("INSERT INTO t VALUES (1), (2), (3), (42)").run();
    expect(await db.prepare("SELECT * FROM t").all()).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }, { x: 42 }]);
    await expect(async () => await db.pull()).rejects.toThrow(/url is empty - sync is paused/);
    url = process.env.VITE_TURSO_DB_URL;
    await db.pull();
    expect(await db.prepare("SELECT * FROM t").all()).toEqual([{ x: 100 }, { x: 1 }, { x: 2 }, { x: 3 }, { x: 42 }]);
})

test('encryption sync', async () => {
    const KEY = 'l/FWopMfZisTLgBX4A42AergrCrYKjiO3BfkJUwv83I=';
    const URL = 'http://encrypted--a--a.localhost:10000';
    {
        const db = await connect({ path: ':memory:', url: URL, remoteEncryption: { key: KEY, cipher: 'aes256gcm' } });
        await db.exec("CREATE TABLE IF NOT EXISTS t(x)");
        await db.exec("DELETE FROM t");
        await db.push();
        await db.close();
    }
    const db1 = await connect({ path: ':memory:', url: URL, remoteEncryption: { key: KEY, cipher: 'aes256gcm' } });
    const db2 = await connect({ path: ':memory:', url: URL, remoteEncryption: { key: KEY, cipher: 'aes256gcm' } });
    await db1.exec("INSERT INTO t VALUES (1), (2), (3)");
    await db2.exec("INSERT INTO t VALUES (4), (5), (6)");
    expect(await db1.prepare("SELECT * FROM t").all()).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }]);
    expect(await db2.prepare("SELECT * FROM t").all()).toEqual([{ x: 4 }, { x: 5 }, { x: 6 }]);
    await Promise.all([db1.push(), db2.push()]);
    await Promise.all([db1.pull(), db2.pull()]);
    const expected = [{ x: 1 }, { x: 2 }, { x: 3 }, { x: 4 }, { x: 5 }, { x: 6 }];
    expect((await db1.prepare("SELECT * FROM t").all()).sort(intCompare)).toEqual(expected.sort(intCompare));
    expect((await db2.prepare("SELECT * FROM t").all()).sort(intCompare)).toEqual(expected.sort(intCompare));
});

test('defered encryption sync', async () => {
    const URL = 'http://encrypted--a--a.localhost:10000';
    const KEY = 'l/FWopMfZisTLgBX4A42AergrCrYKjiO3BfkJUwv83I=';
    let url = null;
    {
        const db = await connect({ path: ':memory:', url: URL, remoteEncryption: { key: KEY, cipher: 'aes256gcm' } });
        await db.exec("CREATE TABLE IF NOT EXISTS t(x)");
        await db.exec("DELETE FROM t");
        await db.exec("INSERT INTO t VALUES (100)");
        await db.push();
        await db.close();
    }
    const db = await connect({ path: ':memory:', url: () => url, remoteEncryption: { key: KEY, cipher: 'aes256gcm' } });
    await db.exec("CREATE TABLE IF NOT EXISTS t(x)");
    await db.exec("INSERT INTO t VALUES (1), (2), (3)");
    expect(await db.prepare("SELECT * FROM t").all()).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }]);

    url = URL;
    await db.pull();

    const expected = [{ x: 100 }, { x: 1 }, { x: 2 }, { x: 3 }];
    expect((await db.prepare("SELECT * FROM t").all())).toEqual(expected);
});

test('select-after-push', async () => {
    {
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
        await db.exec("CREATE TABLE IF NOT EXISTS t(x)");
        await db.exec("DELETE FROM t");
        await db.push();
        await db.close();
    }
    {
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
        await db.exec("INSERT INTO t VALUES (1), (2), (3)");
        await db.push();
    }
    {
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
        const rows = await db.prepare('SELECT * FROM t').all();
        expect(rows).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }])
    }
})

test('select-without-push', async () => {
    {
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
        await db.exec("CREATE TABLE IF NOT EXISTS t(x)");
        await db.exec("DELETE FROM t");
        await db.push();
        await db.close();
    }
    {
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
        await db.exec("INSERT INTO t VALUES (1), (2), (3)");
    }
    {
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
        const rows = await db.prepare('SELECT * FROM t').all();
        expect(rows).toEqual([])
    }
})

test('merge-non-overlapping-keys', async () => {
    {
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
        await db.exec("CREATE TABLE IF NOT EXISTS q(x TEXT PRIMARY KEY, y)");
        await db.exec("DELETE FROM q");
        await db.push();
        await db.close();
    }
    const db1 = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
    await db1.exec("INSERT INTO q VALUES ('k1', 'value1'), ('k2', 'value2')");

    const db2 = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
    await db2.exec("INSERT INTO q VALUES ('k3', 'value3'), ('k4', 'value4'), ('k5', 'value5')");

    await Promise.all([db1.push(), db2.push()]);
    await Promise.all([db1.pull(), db2.pull()]);

    const rows1 = await db1.prepare('SELECT * FROM q').all();
    const rows2 = await db1.prepare('SELECT * FROM q').all();
    const expected = [{ x: 'k1', y: 'value1' }, { x: 'k2', y: 'value2' }, { x: 'k3', y: 'value3' }, { x: 'k4', y: 'value4' }, { x: 'k5', y: 'value5' }];
    expect(rows1.sort(localeCompare)).toEqual(expected.sort(localeCompare))
    expect(rows2.sort(localeCompare)).toEqual(expected.sort(localeCompare))
})

test('last-push-wins', async () => {
    {
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
        await db.exec("CREATE TABLE IF NOT EXISTS q(x TEXT PRIMARY KEY, y)");
        await db.exec("DELETE FROM q");
        await db.push();
        await db.close();
    }
    const db1 = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
    await db1.exec("INSERT INTO q VALUES ('k1', 'value1'), ('k2', 'value2'), ('k4', 'value4')");

    const db2 = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
    await db2.exec("INSERT INTO q VALUES ('k1', 'value3'), ('k2', 'value4'), ('k3', 'value5')");

    await db2.push();
    await db1.push();
    await Promise.all([db1.pull(), db2.pull()]);

    const rows1 = await db1.prepare('SELECT * FROM q').all();
    const rows2 = await db1.prepare('SELECT * FROM q').all();
    const expected = [{ x: 'k1', y: 'value1' }, { x: 'k2', y: 'value2' }, { x: 'k3', y: 'value5' }, { x: 'k4', y: 'value4' }];
    expect(rows1.sort(localeCompare)).toEqual(expected.sort(localeCompare))
    expect(rows2.sort(localeCompare)).toEqual(expected.sort(localeCompare))
})

test('last-push-wins-with-delete', async () => {
    {
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
        await db.exec("CREATE TABLE IF NOT EXISTS q(x TEXT PRIMARY KEY, y)");
        await db.exec("DELETE FROM q");
        await db.push();
        await db.close();
    }
    const db1 = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
    await db1.exec("INSERT INTO q VALUES ('k1', 'value1'), ('k2', 'value2'), ('k4', 'value4')");
    await db1.exec("DELETE FROM q")

    const db2 = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
    await db2.exec("INSERT INTO q VALUES ('k1', 'value3'), ('k2', 'value4'), ('k3', 'value5')");

    await db2.push();
    await db1.push();
    await Promise.all([db1.pull(), db2.pull()]);

    const rows1 = await db1.prepare('SELECT * FROM q').all();
    const rows2 = await db1.prepare('SELECT * FROM q').all();
    const expected = [{ x: 'k3', y: 'value5' }];
    expect(rows1).toEqual(expected)
    expect(rows2).toEqual(expected)
})

test('constraint-conflict', async () => {
    {
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
        await db.exec("CREATE TABLE IF NOT EXISTS u(x TEXT PRIMARY KEY, y UNIQUE)");
        await db.exec("DELETE FROM u");
        await db.push();
        await db.close();
    }
    const db1 = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
    await db1.exec("INSERT INTO u VALUES ('k1', 'value1')");

    const db2 = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
    await db2.exec("INSERT INTO u VALUES ('k2', 'value1')");

    await db1.push();
    await expect(async () => await db2.push()).rejects.toThrow('SQLite error: UNIQUE constraint failed: u.y');
})

test('checkpoint', async () => {
    {
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
        await db.exec("CREATE TABLE IF NOT EXISTS q(x TEXT PRIMARY KEY, y)");
        await db.exec("DELETE FROM q");
        await db.push();
        await db.close();
    }
    const db1 = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
    for (let i = 0; i < 1000; i++) {
        await db1.exec(`INSERT INTO q VALUES ('k${i}', 'v${i}')`);
    }
    expect((await db1.stats()).mainWalSize).toBeGreaterThan(4096 * 1000);
    await db1.checkpoint();
    expect((await db1.stats()).mainWalSize).toBe(0);
    let revertWal = (await db1.stats()).revertWalSize;
    expect(revertWal).toBeLessThan(4096 * 1000 / 100);

    for (let i = 0; i < 1000; i++) {
        await db1.exec(`UPDATE q SET y = 'u${i}' WHERE x = 'k${i}'`);
    }
    await db1.checkpoint();
    expect((await db1.stats()).revertWalSize).toBe(revertWal);
})

test('persistence-push', async () => {
    {
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
        await db.exec("CREATE TABLE IF NOT EXISTS q(x TEXT PRIMARY KEY, y)");
        await db.exec("DELETE FROM q");
        await db.push();
        await db.close();
    }
    const path = `test-${(Math.random() * 10000) | 0}.db`;
    {
        const db1 = await connect({ path: path, url: process.env.VITE_TURSO_DB_URL });
        await db1.exec(`INSERT INTO q VALUES ('k1', 'v1')`);
        await db1.exec(`INSERT INTO q VALUES ('k2', 'v2')`);
        await db1.close();
    }

    {
        const db2 = await connect({ path: path, url: process.env.VITE_TURSO_DB_URL });
        await db2.exec(`INSERT INTO q VALUES ('k3', 'v3')`);
        await db2.exec(`INSERT INTO q VALUES ('k4', 'v4')`);
        const stmt = db2.prepare('SELECT * FROM q');
        const rows = await stmt.all();
        const expected = [{ x: 'k1', y: 'v1' }, { x: 'k2', y: 'v2' }, { x: 'k3', y: 'v3' }, { x: 'k4', y: 'v4' }];
        expect(rows).toEqual(expected)
        stmt.close();
        await db2.close();
    }

    {
        const db3 = await connect({ path: path, url: process.env.VITE_TURSO_DB_URL });
        await db3.push();
        await db3.close();
    }

    {
        const db4 = await connect({ path: path, url: process.env.VITE_TURSO_DB_URL });
        const rows = await db4.prepare('SELECT * FROM q').all();
        const expected = [{ x: 'k1', y: 'v1' }, { x: 'k2', y: 'v2' }, { x: 'k3', y: 'v3' }, { x: 'k4', y: 'v4' }];
        expect(rows).toEqual(expected)
        await db4.close();
    }
})

test('persistence-offline', async () => {
    {
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
        await db.exec("CREATE TABLE IF NOT EXISTS q(x TEXT PRIMARY KEY, y)");
        await db.exec("DELETE FROM q");
        await db.push();
        await db.close();
    }
    const path = `test-${(Math.random() * 10000) | 0}.db`;
    {
        const db = await connect({ path: path, url: process.env.VITE_TURSO_DB_URL });
        await db.exec(`INSERT INTO q VALUES ('k1', 'v1')`);
        await db.exec(`INSERT INTO q VALUES ('k2', 'v2')`);
        await db.push();
        await db.close();
    }
    {
        const db = await connect({ path: path, url: "https://not-valid-url.localhost" });
        const rows = await db.prepare("SELECT * FROM q").all();
        const expected = [{ x: 'k1', y: 'v1' }, { x: 'k2', y: 'v2' }];
        expect(rows.sort(localeCompare)).toEqual(expected.sort(localeCompare))
        await db.close();
    }
})

test('persistence-pull-push', async () => {
    {
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
        await db.exec("CREATE TABLE IF NOT EXISTS q(x TEXT PRIMARY KEY, y)");
        await db.exec("DELETE FROM q");
        await db.push();
        await db.close();
    }
    const path1 = `test-${(Math.random() * 10000) | 0}.db`;
    const path2 = `test-${(Math.random() * 10000) | 0}.db`;
    const db1 = await connect({ path: path1, url: process.env.VITE_TURSO_DB_URL });
    await db1.exec(`INSERT INTO q VALUES ('k1', 'v1')`);
    await db1.exec(`INSERT INTO q VALUES ('k2', 'v2')`);
    const stats1 = await db1.stats();

    const db2 = await connect({ path: path2, url: process.env.VITE_TURSO_DB_URL });
    await db2.exec(`INSERT INTO q VALUES ('k3', 'v3')`);
    await db2.exec(`INSERT INTO q VALUES ('k4', 'v4')`);

    await Promise.all([db1.push(), db2.push()]);
    await Promise.all([db1.pull(), db2.pull()]);
    const stats2 = await db1.stats();
    console.info(stats1, stats2);
    expect(stats1.revision).not.toBe(stats2.revision);

    const rows1 = await db1.prepare('SELECT * FROM q').all();
    const rows2 = await db2.prepare('SELECT * FROM q').all();
    const expected = [{ x: 'k1', y: 'v1' }, { x: 'k2', y: 'v2' }, { x: 'k3', y: 'v3' }, { x: 'k4', y: 'v4' }];
    expect(rows1.sort(localeCompare)).toEqual(expected.sort(localeCompare))
    expect(rows2.sort(localeCompare)).toEqual(expected.sort(localeCompare))
})

test('pull-push-concurrent', async () => {
    {
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL, longPollTimeoutMs: 5000 });
        await db.exec("CREATE TABLE IF NOT EXISTS q(x TEXT PRIMARY KEY, y)");
        await db.exec("DELETE FROM q");
        await db.push();
        await db.close();
    }
    let pullResolve = null;
    const pullFinish = new Promise(resolve => pullResolve = resolve);
    let pushResolve = null;
    const pushFinish = new Promise(resolve => pushResolve = resolve);
    let stopPull = false;
    let stopPush = false;
    const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL });
    let pull = async () => {
        try {
            await db.pull();
        } catch (e) {
            console.error('pull', e);
        } finally {
            if (!stopPull) {
                setTimeout(pull, 0);
            } else {
                pullResolve()
            }
        }
    }
    let push = async () => {
        try {
            if ((await db.stats()).cdcOperations > 0) {
                await db.push();
            }
        } catch (e) {
            console.error('push', e);
        } finally {
            if (!stopPush) {
                setTimeout(push, 0);
            } else {
                pushResolve();
            }
        }
    }
    setTimeout(pull, 0);
    setTimeout(push, 0);
    for (let i = 0; i < 1000; i++) {
        await db.exec(`INSERT INTO q VALUES ('k${i}', 'v${i}')`);
    }
    await new Promise(resolve => setTimeout(resolve, 1000));
    stopPush = true;
    await pushFinish;
    stopPull = true;
    await pullFinish;
    console.info(await db.stats());
})

test('concurrent-updates', { timeout: 60000 }, async () => {
    {
        const db = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL, longPollTimeoutMs: 10 });
        await db.exec("CREATE TABLE IF NOT EXISTS three(x TEXT PRIMARY KEY, y, z)");
        await db.exec("DELETE FROM three");
        await db.push();
        await db.close();
    }
    let stop = false;
    const dbs = [];
    for (let i = 0; i < 8; i++) {
        dbs.push(await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL }));
    }
    async function pull(db, i) {
        try {
            await db.pull();
        } catch (e) {
            console.error('pull', i, e);
        } finally {
            if (!stop) {
                setTimeout(async () => await pull(db, i), 0);
            }
        }
    }
    async function push(db, i) {
        try {
            await db.push();
        } catch (e) {
            console.error('push', i, e);
        } finally {
            if (!stop) {
                setTimeout(async () => await push(db, i), 0);
            }
        }
    }
    for (let i = 0; i < dbs.length; i++) {
        setTimeout(async () => await pull(dbs[i], i), 0)
        setTimeout(async () => await push(dbs[i], i), 0)
    }
    for (let i = 0; i < 1000; i++) {
        try {
            const tasks = [];
            for (let s = 0; s < dbs.length; s++) {
                tasks.push(dbs[s].exec(`INSERT INTO three VALUES ('${s}', 0, randomblob(128)) ON CONFLICT DO UPDATE SET y = y + 1, z = randomblob(128)`));
            }
            await Promise.all(tasks);
        } catch (e) {
            // ignore
        }
        await new Promise(resolve => setTimeout(resolve, 1));
    }
    stop = true;
    await Promise.all(dbs.map(db => db.push()));
    await Promise.all(dbs.map(db => db.pull()));
    let results = [];
    for (let i = 0; i < dbs.length; i++) {
        results.push(await dbs[i].prepare('SELECT x, y FROM three').all());
    }
    for (let i = 0; i < dbs.length; i++) {
        expect(results[i]).toEqual(results[0]);
        for (let s = 0; s < dbs.length; s++) {
            expect(results[i][s].y).toBeGreaterThan(500);
        }
    }
})

test('transform', async () => {
    {
        const db = await connect({
            path: ':memory:',
            url: process.env.VITE_TURSO_DB_URL,
        });
        await db.exec("CREATE TABLE IF NOT EXISTS counter(key TEXT PRIMARY KEY, value INTEGER)");
        await db.exec("DELETE FROM counter");
        await db.exec("INSERT INTO counter VALUES ('1', 0)")
        await db.push();
        await db.close();
    }
    const transform = (m: DatabaseRowMutation) => ({
        operation: 'rewrite',
        stmt: {
            sql: `UPDATE counter SET value = value + ? WHERE key = ?`,
            values: [m.after.value - m.before.value, m.after.key]
        }
    } as DatabaseRowTransformResult);
    const db1 = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL, transform: transform });
    const db2 = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL, transform: transform });

    await db1.exec("UPDATE counter SET value = value + 1 WHERE key = '1'");
    await db2.exec("UPDATE counter SET value = value + 1 WHERE key = '1'");

    await Promise.all([db1.push(), db2.push()]);
    await Promise.all([db1.pull(), db2.pull()]);

    const rows1 = await db1.prepare('SELECT * FROM counter').all();
    const rows2 = await db2.prepare('SELECT * FROM counter').all();
    expect(rows1).toEqual([{ key: '1', value: 2 }]);
    expect(rows2).toEqual([{ key: '1', value: 2 }]);
})

test('transform-many', async () => {
    {
        const db = await connect({
            path: ':memory:',
            url: process.env.VITE_TURSO_DB_URL,
        });
        await db.exec("CREATE TABLE IF NOT EXISTS counter(key TEXT PRIMARY KEY, value INTEGER)");
        await db.exec("DELETE FROM counter");
        await db.exec("INSERT INTO counter VALUES ('1', 0)")
        await db.push();
        await db.close();
    }
    const transform = (m: DatabaseRowMutation) => ({
        operation: 'rewrite',
        stmt: {
            sql: `UPDATE counter SET value = value + ? WHERE key = ?`,
            values: [m.after.value - m.before.value, m.after.key]
        }
    } as DatabaseRowTransformResult);
    const db1 = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL, transform: transform });
    const db2 = await connect({ path: ':memory:', url: process.env.VITE_TURSO_DB_URL, transform: transform });

    for (let i = 0; i < 1002; i++) {
        await db1.exec("UPDATE counter SET value = value + 1 WHERE key = '1'");
    }
    for (let i = 0; i < 1001; i++) {
        await db2.exec("UPDATE counter SET value = value + 1 WHERE key = '1'");
    }

    let start = performance.now();
    await Promise.all([db1.push(), db2.push()]);
    console.info('push', performance.now() - start);

    start = performance.now();
    await Promise.all([db1.pull(), db2.pull()]);
    console.info('pull', performance.now() - start);

    const rows1 = await db1.prepare('SELECT * FROM counter').all();
    const rows2 = await db2.prepare('SELECT * FROM counter').all();
    expect(rows1).toEqual([{ key: '1', value: 1001 + 1002 }]);
    expect(rows2).toEqual([{ key: '1', value: 1001 + 1002 }]);
})
