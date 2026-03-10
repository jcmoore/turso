import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { access, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn, type ChildProcess } from "node:child_process";
import { createServer } from "node:net";

import { DatabasePromise } from "@tursodatabase/database-common";

import { S3PageClient } from "../common/s3-page.js";
import { connect } from "./promise.js";
import { Database as NativeDatabase, EncryptionCipher, decryptS3PageMetadata, encryptS3PageMetadata } from "#index";

const SEAWEED_READY_TIMEOUT_MS = 60_000;
const SEAWEED_POLL_INTERVAL_MS = 250;
const MIN_TEST_PORT = 20_000;
const MAX_TEST_PORT = 50_000;
const ACCESS_KEY_ID = "admin";
const SECRET_ACCESS_KEY = "admin_secret_key";
const ENCRYPTION_KEY = Buffer.alloc(32, 7).toString("base64");
const ENCRYPTION_HEXKEY = Buffer.from(ENCRYPTION_KEY, "base64").toString("hex");

function pageSizeFromDatabaseBytes(bytes: Buffer): number {
  const value = bytes.readUInt16BE(16);
  return value === 1 ? 65536 : value;
}

function encodeSegment(pages: Buffer[]): Uint8Array {
  const totalBytes = pages.reduce((size, page) => size + 8 + page.byteLength, 12);
  const payload = Buffer.alloc(totalBytes);
  let offset = 0;
  payload.write("TS3P", offset, "utf8");
  offset += 4;
  payload.writeUInt32BE(1, offset);
  offset += 4;
  payload.writeUInt32BE(pages.length, offset);
  offset += 4;
  for (let index = 0; index < pages.length; index += 1) {
    payload.writeUInt32BE(index + 1, offset);
    offset += 4;
    payload.writeUInt32BE(pages[index].byteLength, offset);
    offset += 4;
    pages[index].copy(payload, offset);
    offset += pages[index].byteLength;
  }
  return payload;
}

async function openEncryptedLocalDb(path: string): Promise<DatabasePromise> {
  const db = new DatabasePromise(
    new NativeDatabase(path, {
      encryption: {
        cipher: EncryptionCipher.Aegis256,
        hexkey: ENCRYPTION_HEXKEY,
      },
    }) as any,
  );
  await db.connect();
  return db;
}

async function snapshotDatabasePages(path: string): Promise<Buffer[]> {
  const bytes = await readFile(path);
  const pageSize = pageSizeFromDatabaseBytes(bytes);
  const pages: Buffer[] = [];
  for (let offset = 0; offset < bytes.byteLength; offset += pageSize) {
    pages.push(bytes.subarray(offset, offset + pageSize));
  }
  return pages;
}

async function publishRemoteSnapshot(opts: {
  client: S3PageClient;
  endpoint: string;
  bucket: string;
  prefix: string;
  commitId: string;
  parentCommitId: string | null;
  sequenceNo: number;
  pages: Buffer[];
  headEtag: string | null;
}): Promise<string> {
  const databaseId = `s3_page_v1:${new URL(opts.endpoint).toString()}/${opts.bucket}/${opts.prefix}`;
  const segmentId = `${opts.commitId}-segment-0`;
  await opts.client.putSegment(segmentId, encodeSegment(opts.pages));
  await opts.client.putCommit(
    opts.commitId,
    encryptS3PageMetadata(
      Buffer.from(
        JSON.stringify({
          version: 1,
          database_id: databaseId,
          commit_id: opts.commitId,
          parent_commit_id: opts.parentCommitId,
          sequence_no: opts.sequenceNo,
          page_count: opts.pages.length,
          client_id: "seed",
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
    Buffer.from(
      JSON.stringify({
        version: 1,
        database_id: databaseId,
        head_commit_id: opts.commitId,
        sequence_no: opts.sequenceNo,
      }),
    ),
    EncryptionCipher.Aegis256,
    ENCRYPTION_HEXKEY,
    "refs/head",
  );
  if (opts.headEtag == null) {
    const created = await opts.client.putObjectIfAbsent("refs/head", headBytes);
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
  const objectKeyFor = (url: URL) => url.pathname.replace(/^\/+/, "");

  const fetchImpl: typeof fetch = async (input, init) => {
    const url = new URL(typeof input === "string" ? input : input instanceof URL ? input.href : input.url);
    const method = (init?.method ?? "GET").toUpperCase();
    const pathname = url.pathname.replace(/^\/+/, "");
    const headers = new Headers(init?.headers ?? {});
    const pathParts = pathname.split("/").filter(Boolean);

    if (method === "PUT" && pathParts.length === 1) {
      const bucket = pathParts[0];
      if (buckets.has(bucket)) {
        return new Response(null, { status: 409 });
      }
      buckets.add(bucket);
      return new Response(null, { status: 200 });
    }

    const key = objectKeyFor(url);
    const current = objects.get(key);
    if (method === "HEAD") {
      if (current == null) {
        return new Response(null, { status: 404 });
      }
      return new Response(null, {
        status: 200,
        headers: current.etag == null ? undefined : { etag: `"${current.etag}"` },
      });
    }
    if (method === "GET") {
      if (current == null) {
        return new Response(null, { status: 404 });
      }
      return new Response(current.body, {
        status: 200,
        headers: {
          etag: `"${current.etag}"`,
          ...(current.contentType == null ? {} : { "content-type": current.contentType }),
        },
      });
    }
    if (method === "PUT") {
      const ifNoneMatch = headers.get("if-none-match");
      const ifMatch = headers.get("if-match")?.replace(/^"|"$/g, "") ?? null;
      if (ifNoneMatch === "*" && current != null) {
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
        contentType: headers.get("content-type"),
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

async function findFreePort(): Promise<number> {
  for (let attempt = 0; attempt < 128; attempt += 1) {
    const candidate =
      MIN_TEST_PORT + Math.floor(Math.random() * (MAX_TEST_PORT - MIN_TEST_PORT));
    const port = await new Promise<number | null>((resolvePort) => {
      const server = createServer();
      server.once("error", () => resolvePort(null));
      server.listen(candidate, "127.0.0.1", () => {
        server.close((err) => {
          if (err != null) {
            resolvePort(null);
            return;
          }
          resolvePort(candidate);
        });
      });
    });
    if (port != null) {
      return port;
    }
  }
  throw new Error("unable to allocate free test port in SeaweedFS-safe range");
}

async function waitForSeaweedfs(
  endpoint: string,
  child: ChildProcess,
  output: () => string,
): Promise<void> {
  const deadline = Date.now() + SEAWEED_READY_TIMEOUT_MS;
  let lastError: unknown = null;
  while (Date.now() < deadline) {
    if (child.exitCode != null) {
      throw new Error(
        `SeaweedFS exited before becoming ready at ${endpoint} (code ${child.exitCode}): ${output()}`,
      );
    }
    try {
      const response = await fetch(endpoint);
      if (response.status > 0) {
        return;
      }
    } catch (error) {
      lastError = error;
    }
    await new Promise((resolveWait) => setTimeout(resolveWait, SEAWEED_POLL_INTERVAL_MS));
  }
  throw new Error(
    `SeaweedFS did not become ready at ${endpoint}: ${String(lastError)}\n${output()}`,
  );
}

async function pathExists(path: string): Promise<boolean> {
  try {
    await access(path);
    return true;
  } catch (error) {
    return (error as NodeJS.ErrnoException).code !== "ENOENT";
  }
}

async function findSeaweedfsRoot(): Promise<string> {
  const here = dirname(fileURLToPath(import.meta.url));
  let current = here;
  while (true) {
    const candidate = join(current, "modules", "github.com", "seaweedfs", "seaweedfs", "v", "4");
    if (await pathExists(candidate)) {
      return candidate;
    }
    const parent = dirname(current);
    if (parent === current) {
      break;
    }
    current = parent;
  }
  throw new Error("unable to locate SeaweedFS submodule");
}

describe("S3PageClient SeaweedFS integration", () => {
  let child: ChildProcess | null = null;
  let dataDir = "";
  let endpoint = "";

  beforeAll(async () => {
    const seaweedRoot = await findSeaweedfsRoot();
    const masterPort = await findFreePort();
    const volumePort = await findFreePort();
    const filerPort = await findFreePort();
    const s3Port = await findFreePort();
    endpoint = `http://127.0.0.1:${s3Port}`;

    dataDir = await mkdtemp(join(tmpdir(), "turso-s3-page-seaweedfs-"));
    const configPath = join(dataDir, "s3.json");
    await writeFile(
      configPath,
      JSON.stringify(
        {
          identities: [
            {
              name: "admin",
              credentials: [{ accessKey: ACCESS_KEY_ID, secretKey: SECRET_ACCESS_KEY }],
              actions: ["Admin", "Read", "Write"],
            },
          ],
        },
        null,
        2,
      ),
      "utf8",
    );

    child = spawn(
      "go",
      [
        "run",
        "./weed",
        "server",
        `-dir=${dataDir}`,
        "-ip=127.0.0.1",
        "-s3",
        `-master.port=${masterPort}`,
        `-volume.port=${volumePort}`,
        `-filer.port=${filerPort}`,
        `-s3.port=${s3Port}`,
        "-s3.port.iceberg=0",
        `-s3.config=${configPath}`,
      ],
      {
        cwd: seaweedRoot,
        stdio: "pipe",
      },
    );

    let stdout = "";
    let stderr = "";
    child.stdout?.on("data", (chunk: Buffer) => {
      stdout += chunk.toString("utf8");
    });
    child.stderr?.on("data", (chunk: Buffer) => {
      stderr += chunk.toString("utf8");
    });

    await waitForSeaweedfs(endpoint, child, () => `${stdout}\n${stderr}`.trim());
    if (child.exitCode != null) {
      throw new Error(
        `SeaweedFS exited before test startup completed: ${stdout}\n${stderr}`.trim(),
      );
    }
  }, 90_000);

  afterAll(async () => {
    if (child != null && child.exitCode == null) {
      child.kill("SIGTERM");
      await new Promise((resolveWait) => child?.once("exit", () => resolveWait(undefined)));
    }
    if (dataDir !== "") {
      await rm(dataDir, { recursive: true, force: true });
    }
  });

  test("creates immutable objects and enforces head CAS", async () => {
    const client = new S3PageClient({
      backend: "s3_page_v1",
      endpoint,
      bucket: `suite-${Date.now()}`,
      prefix: "db/main",
      credentials: async () => ({
        accessKeyId: ACCESS_KEY_ID,
        secretAccessKey: SECRET_ACCESS_KEY,
      }),
      remoteEncryption: {
        key: ENCRYPTION_KEY,
        cipher: "aegis256",
      },
    });

    await client.createBucket();

    await expect(client.putSegment("segment-a", Uint8Array.of(1, 2, 3))).resolves.toEqual({
      etag: expect.any(String),
    });
    await expect(client.putSegment("segment-a", Uint8Array.of(1, 2, 3))).rejects.toThrow(
      /S3 PUT segments\/segment-a failed: (409|412)/,
    );

    const initialHead = Uint8Array.of(9, 9, 9);
    await expect(client.putObjectIfAbsent("refs/head", initialHead)).resolves.toEqual({
      etag: expect.any(String),
    });

    const headBefore = await client.headObject("refs/head");
    expect(headBefore?.etag).toEqual(expect.any(String));

    const nextHead = Uint8Array.of(8, 8, 8);
    await expect(client.casHead(nextHead, headBefore!.etag!)).resolves.toEqual({
      etag: expect.any(String),
    });

    await expect(client.casHead(Uint8Array.of(7, 7, 7), headBefore!.etag!)).rejects.toThrow(
      /S3 PUT refs\/head failed: (409|412)/,
    );

    const finalHead = await client.getHeadRef();
    expect(finalHead?.body).toEqual(nextHead);
  }, 90_000);

  test("native runtime publishes commits and persists sidecar metadata", async () => {
    const bucket = `runtime-${Date.now()}`;
    const prefix = "db/main";
    const dbPath = join(dataDir, "native-runtime.db");
    const client = new S3PageClient({
      backend: "s3_page_v1",
      endpoint,
      bucket,
      prefix,
      credentials: async () => ({
        accessKeyId: ACCESS_KEY_ID,
        secretAccessKey: SECRET_ACCESS_KEY,
      }),
      remoteEncryption: {
        key: ENCRYPTION_KEY,
        cipher: "aegis256",
      },
    });

    await client.createBucket();

    const db = await connect({
      path: dbPath,
      clientName: "native-runtime",
      sync: {
        backend: "s3_page_v1",
        endpoint,
        bucket,
        prefix,
        credentials: async () => ({
          accessKeyId: ACCESS_KEY_ID,
          secretAccessKey: SECRET_ACCESS_KEY,
        }),
        remoteEncryption: {
          key: ENCRYPTION_KEY,
          cipher: "aegis256",
        },
      },
    });

    await db.exec("CREATE TABLE IF NOT EXISTS entries(value INTEGER)");
    await db.exec("INSERT INTO entries VALUES (42)");
    await expect(db.prepare("SELECT value FROM entries").all()).resolves.toEqual([{ value: 42 }]);
    await db.close();

    const head = await client.getHeadRef();
    expect(head).not.toBeNull();
    expect(() => JSON.parse(new TextDecoder().decode(head!.body))).toThrow();
    expect(() =>
      decryptS3PageMetadata(
        Buffer.from(head!.body),
        EncryptionCipher.Aegis256,
        Buffer.alloc(32, 9).toString("hex"),
        "refs/head",
      ),
    ).toThrow();

    const headManifest = JSON.parse(
      new TextDecoder().decode(
        decryptS3PageMetadata(
          Buffer.from(head!.body),
          EncryptionCipher.Aegis256,
          ENCRYPTION_HEXKEY,
          "refs/head",
        ),
      ),
    );
    expect(headManifest.head_commit_id).toEqual(expect.any(String));
    expect(headManifest.sequence_no).toBeGreaterThanOrEqual(2);

    const commit = await client.getObject(`commits/${headManifest.head_commit_id}`);
    expect(commit).not.toBeNull();
    expect(() => JSON.parse(new TextDecoder().decode(commit!.body))).toThrow();
    expect(() =>
      decryptS3PageMetadata(
        Buffer.from(commit!.body),
        EncryptionCipher.Aegis256,
        Buffer.alloc(32, 9).toString("hex"),
        `commits/${headManifest.head_commit_id}`,
      ),
    ).toThrow();

    const sidecar = JSON.parse(
      await readFile(`${dbPath}-s3-page-info`, { encoding: "utf8" }),
    );
    expect(sidecar.database_id).toContain(bucket);
    expect(sidecar.client_id).toContain("native-runtime");
    expect(sidecar.head_commit_id).toEqual(headManifest.head_commit_id);
    expect(sidecar.sequence_no).toEqual(headManifest.sequence_no);
    expect(sidecar.head_etag).toEqual(head!.etag);
  }, 90_000);

  test("native runtime pull bootstraps and advances a pinned read replica", async () => {
    const mock = createMockS3Fetch();
    const originalFetch = globalThis.fetch;
    globalThis.fetch = mock.fetchImpl;

    try {
    const bucket = `pull-${Date.now()}`;
    const prefix = "db/main";
    const mockEndpoint = "http://mock-s3.invalid";
    const seedPath = join(dataDir, "native-pull-seed.db");
    const readerPath = join(dataDir, "native-pull-reader.db");
    const syncOpts = {
      backend: "s3_page_v1" as const,
      endpoint: mockEndpoint,
      bucket,
      prefix,
      credentials: async () => ({
        accessKeyId: ACCESS_KEY_ID,
        secretAccessKey: SECRET_ACCESS_KEY,
      }),
      remoteEncryption: {
        key: ENCRYPTION_KEY,
        cipher: "aegis256" as const,
      },
    };

    const client = new S3PageClient(syncOpts, { fetchImpl: mock.fetchImpl });
    await client.createBucket();
    const seed = await openEncryptedLocalDb(seedPath);
    await seed.exec("PRAGMA journal_mode = 'mvcc'");
    await seed.exec("CREATE TABLE IF NOT EXISTS entries(value INTEGER)");
    await seed.exec("INSERT INTO entries VALUES (1)");
    await seed.exec("PRAGMA wal_checkpoint(TRUNCATE)");
    await seed.close();
    let headEtag = await publishRemoteSnapshot({
      client,
      endpoint: mockEndpoint,
      bucket,
      prefix,
      commitId: "seed-1",
      parentCommitId: null,
      sequenceNo: 1,
      pages: await snapshotDatabasePages(seedPath),
      headEtag: null,
    });

    const reader = await connect({
      path: readerPath,
      clientName: "reader",
      sync: syncOpts,
    });

    await expect(reader.pull()).resolves.toBe(true);
    await expect(reader.prepare("SELECT value FROM entries ORDER BY value").all()).resolves.toEqual([
      { value: 1 },
    ]);

    const seedNext = await openEncryptedLocalDb(seedPath);
    await seedNext.exec("PRAGMA journal_mode = 'mvcc'");
    await seedNext.exec("INSERT INTO entries VALUES (2)");
    await seedNext.exec("PRAGMA wal_checkpoint(TRUNCATE)");
    await seedNext.close();
    headEtag = await publishRemoteSnapshot({
      client,
      endpoint: mockEndpoint,
      bucket,
      prefix,
      commitId: "seed-2",
      parentCommitId: "seed-1",
      sequenceNo: 2,
      pages: await snapshotDatabasePages(seedPath),
      headEtag,
    });
    expect(headEtag).toEqual(expect.any(String));

    await expect(reader.prepare("SELECT value FROM entries ORDER BY value").all()).resolves.toEqual([
      { value: 1 },
    ]);
    await expect(reader.pull()).resolves.toBe(true);
    await expect(reader.prepare("SELECT value FROM entries ORDER BY value").all()).resolves.toEqual([
      { value: 1 },
      { value: 2 },
    ]);
    await expect(reader.pull()).resolves.toBe(false);

    const readerSidecar = JSON.parse(
      await readFile(`${readerPath}-s3-page-info`, { encoding: "utf8" }),
    );
    expect(readerSidecar.last_pulled_at).toEqual(expect.any(Number));
    expect(readerSidecar.head_commit_id).toEqual(expect.any(String));

    await reader.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  }, 90_000);

  test("native runtime reconnect recovers a pending published head", async () => {
    const mock = createMockS3Fetch();
    const originalFetch = globalThis.fetch;
    globalThis.fetch = mock.fetchImpl;

    try {
      const bucket = `recover-${Date.now()}`;
      const prefix = "db/main";
      const mockEndpoint = "http://mock-s3.invalid";
      const seedPath = join(dataDir, "native-recover-seed.db");
      const readerPath = join(dataDir, "native-recover-reader.db");
      const syncOpts = {
        backend: "s3_page_v1" as const,
        endpoint: mockEndpoint,
        bucket,
        prefix,
        credentials: async () => ({
          accessKeyId: ACCESS_KEY_ID,
          secretAccessKey: SECRET_ACCESS_KEY,
        }),
        remoteEncryption: {
          key: ENCRYPTION_KEY,
          cipher: "aegis256" as const,
        },
      };

      const client = new S3PageClient(syncOpts, { fetchImpl: mock.fetchImpl });
      await client.createBucket();

      const seed = await openEncryptedLocalDb(seedPath);
      await seed.exec("PRAGMA journal_mode = 'mvcc'");
      await seed.exec("CREATE TABLE IF NOT EXISTS entries(value INTEGER)");
      await seed.exec("INSERT INTO entries VALUES (1)");
      await seed.exec("PRAGMA wal_checkpoint(TRUNCATE)");
      await seed.close();

      let headEtag = await publishRemoteSnapshot({
        client,
        endpoint: mockEndpoint,
        bucket,
        prefix,
        commitId: "seed-1",
        parentCommitId: null,
        sequenceNo: 1,
        pages: await snapshotDatabasePages(seedPath),
        headEtag: null,
      });

      const reader = await connect({
        path: readerPath,
        clientName: "reader",
        sync: syncOpts,
      });
      await expect(reader.pull()).resolves.toBe(true);
      await expect(reader.prepare("SELECT value FROM entries ORDER BY value").all()).resolves.toEqual([
        { value: 1 },
      ]);
      await reader.close();

      const seedNext = await openEncryptedLocalDb(seedPath);
      await seedNext.exec("PRAGMA journal_mode = 'mvcc'");
      await seedNext.exec("INSERT INTO entries VALUES (2)");
      await seedNext.exec("PRAGMA wal_checkpoint(TRUNCATE)");
      await seedNext.close();

      headEtag = await publishRemoteSnapshot({
        client,
        endpoint: mockEndpoint,
        bucket,
        prefix,
        commitId: "seed-2",
        parentCommitId: "seed-1",
        sequenceNo: 2,
        pages: await snapshotDatabasePages(seedPath),
        headEtag,
      });
      expect(headEtag).toEqual(expect.any(String));

      const sidecarPath = `${readerPath}-s3-page-info`;
      const staleSidecar = JSON.parse(await readFile(sidecarPath, { encoding: "utf8" }));
      staleSidecar.pending_publish_commit_id = "seed-2";
      await writeFile(sidecarPath, JSON.stringify(staleSidecar), "utf8");

      const recoveredReader = await connect({
        path: readerPath,
        clientName: "reader",
        sync: syncOpts,
      });

      await expect(
        recoveredReader.prepare("SELECT value FROM entries ORDER BY value").all(),
      ).resolves.toEqual([{ value: 1 }, { value: 2 }]);

      const recoveredSidecar = JSON.parse(await readFile(sidecarPath, { encoding: "utf8" }));
      expect(recoveredSidecar.head_commit_id).toBe("seed-2");
      expect(recoveredSidecar.sequence_no).toBe(2);
      expect(recoveredSidecar.pending_publish_commit_id).toBeNull();

      await recoveredReader.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  }, 90_000);

  test("native runtime reconnect clears stale pending publish markers", async () => {
    const mock = createMockS3Fetch();
    const originalFetch = globalThis.fetch;
    globalThis.fetch = mock.fetchImpl;

    try {
      const bucket = `recover-stale-${Date.now()}`;
      const prefix = "db/main";
      const mockEndpoint = "http://mock-s3.invalid";
      const seedPath = join(dataDir, "native-recover-stale-seed.db");
      const readerPath = join(dataDir, "native-recover-stale-reader.db");
      const syncOpts = {
        backend: "s3_page_v1" as const,
        endpoint: mockEndpoint,
        bucket,
        prefix,
        credentials: async () => ({
          accessKeyId: ACCESS_KEY_ID,
          secretAccessKey: SECRET_ACCESS_KEY,
        }),
        remoteEncryption: {
          key: ENCRYPTION_KEY,
          cipher: "aegis256" as const,
        },
      };

      const client = new S3PageClient(syncOpts, { fetchImpl: mock.fetchImpl });
      await client.createBucket();

      const seed = await openEncryptedLocalDb(seedPath);
      await seed.exec("PRAGMA journal_mode = 'mvcc'");
      await seed.exec("CREATE TABLE IF NOT EXISTS entries(value INTEGER)");
      await seed.exec("INSERT INTO entries VALUES (1)");
      await seed.exec("PRAGMA wal_checkpoint(TRUNCATE)");
      await seed.close();

      await publishRemoteSnapshot({
        client,
        endpoint: mockEndpoint,
        bucket,
        prefix,
        commitId: "seed-1",
        parentCommitId: null,
        sequenceNo: 1,
        pages: await snapshotDatabasePages(seedPath),
        headEtag: null,
      });

      const reader = await connect({
        path: readerPath,
        clientName: "reader",
        sync: syncOpts,
      });
      await expect(reader.pull()).resolves.toBe(true);
      await reader.close();

      const sidecarPath = `${readerPath}-s3-page-info`;
      const staleSidecar = JSON.parse(await readFile(sidecarPath, { encoding: "utf8" }));
      staleSidecar.pending_publish_commit_id = "never-published";
      await writeFile(sidecarPath, JSON.stringify(staleSidecar), "utf8");

      const recoveredReader = await connect({
        path: readerPath,
        clientName: "reader",
        sync: syncOpts,
      });

      await expect(
        recoveredReader.prepare("SELECT value FROM entries ORDER BY value").all(),
      ).resolves.toEqual([{ value: 1 }]);

      const recoveredSidecar = JSON.parse(await readFile(sidecarPath, { encoding: "utf8" }));
      expect(recoveredSidecar.head_commit_id).toBe("seed-1");
      expect(recoveredSidecar.sequence_no).toBe(1);
      expect(recoveredSidecar.pending_publish_commit_id).toBeNull();

      await recoveredReader.close();
    } finally {
      globalThis.fetch = originalFetch;
    }
  }, 90_000);

});
