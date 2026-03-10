import type { S3PageSyncCredentials, S3PageSyncOpts } from "./types.js";

export interface S3PageObjectHead {
  etag: string | null;
}

export interface S3PageObject {
  body: Uint8Array;
  etag: string | null;
  contentType: string | null;
}

type FetchLike = typeof fetch;

function stripTrailingSlash(value: string): string {
  return value.endsWith("/") ? value.slice(0, -1) : value;
}

function trimSlashes(value: string): string {
  return value.replace(/^\/+|\/+$/g, "");
}

function encodePathSegment(value: string): string {
  return encodeURIComponent(value).replace(/[!'()*]/g, (ch) =>
    `%${ch.charCodeAt(0).toString(16).toUpperCase()}`,
  );
}

function encodeObjectKey(value: string): string {
  return trimSlashes(value)
    .split("/")
    .filter(Boolean)
    .map(encodePathSegment)
    .join("/");
}

function buildObjectPath(bucket: string, prefix: string, key: string): string {
  const pieces = [
    encodePathSegment(trimSlashes(bucket)),
    encodeObjectKey(prefix),
    encodeObjectKey(key),
  ].filter(Boolean);
  return `/${pieces.join("/")}`;
}

function buildBucketPath(bucket: string): string {
  const trimmed = trimSlashes(bucket);
  return `/${encodePathSegment(trimmed)}`;
}

function toHex(bytes: Uint8Array): string {
  return Array.from(bytes, (value) => value.toString(16).padStart(2, "0")).join("");
}

function toUtf8(value: string): Uint8Array {
  return new TextEncoder().encode(value);
}

function toArrayBufferView(bytes: Uint8Array): Uint8Array<ArrayBuffer> {
  const copied = new Uint8Array(bytes.byteLength);
  copied.set(bytes);
  return copied;
}

async function sha256(bytes: Uint8Array): Promise<Uint8Array> {
  const digest = await crypto.subtle.digest("SHA-256", toArrayBufferView(bytes));
  return new Uint8Array(digest);
}

async function hmacSha256(key: Uint8Array, value: string): Promise<Uint8Array> {
  const cryptoKey = await crypto.subtle.importKey(
    "raw",
    toArrayBufferView(key),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const signature = await crypto.subtle.sign(
    "HMAC",
    cryptoKey,
    toArrayBufferView(toUtf8(value)),
  );
  return new Uint8Array(signature);
}

async function deriveSigningKey(
  secretAccessKey: string,
  dateStamp: string,
  region: string,
): Promise<Uint8Array> {
  const kDate = await hmacSha256(toUtf8(`AWS4${secretAccessKey}`), dateStamp);
  const kRegion = await hmacSha256(kDate, region);
  const kService = await hmacSha256(kRegion, "s3");
  return await hmacSha256(kService, "aws4_request");
}

function formatAmzDate(now: Date): string {
  return now.toISOString().replace(/[:-]|\.\d{3}/g, "");
}

function formatDateStamp(now: Date): string {
  return formatAmzDate(now).slice(0, 8);
}

function normalizeHeaderValue(value: string): string {
  return value.trim().replace(/\s+/g, " ");
}

export class S3PageClient {
  readonly endpoint: string;
  readonly bucket: string;
  readonly prefix: string;
  readonly region: string;

  #credentials: S3PageSyncOpts["credentials"];
  #fetchImpl: FetchLike;
  #clock: () => Date;

  constructor(
    opts: S3PageSyncOpts,
    extras?: {
      fetchImpl?: FetchLike;
      clock?: () => Date;
    },
  ) {
    this.endpoint = stripTrailingSlash(opts.endpoint);
    this.bucket = opts.bucket;
    this.prefix = opts.prefix;
    this.region = opts.region ?? "us-east-1";
    this.#credentials = opts.credentials;
    this.#fetchImpl = extras?.fetchImpl ?? fetch;
    this.#clock = extras?.clock ?? (() => new Date());
  }

  objectUrl(key: string): string {
    return `${this.endpoint}${buildObjectPath(this.bucket, this.prefix, key)}`;
  }

  bucketUrl(): string {
    return `${this.endpoint}${buildBucketPath(this.bucket)}`;
  }

  async createBucket(): Promise<void> {
    const response = await this.#signedFetchToPath(
      "PUT",
      buildBucketPath(this.bucket),
      undefined,
      {},
    );
    if (response.ok || response.status === 409) {
      return;
    }
    throw new Error(`S3 create bucket ${this.bucket} failed: ${response.status}`);
  }

  async headObject(key: string): Promise<S3PageObjectHead | null> {
    const response = await this.#signedFetch("HEAD", key, undefined, {});
    if (response.status === 404) {
      return null;
    }
    if (!response.ok) {
      throw new Error(`S3 HEAD ${key} failed: ${response.status}`);
    }
    return {
      etag: response.headers.get("etag")?.replace(/^"|"$/g, "") ?? null,
    };
  }

  async getObject(key: string): Promise<S3PageObject | null> {
    const response = await this.#signedFetch("GET", key, undefined, {});
    if (response.status === 404) {
      return null;
    }
    if (!response.ok) {
      throw new Error(`S3 GET ${key} failed: ${response.status}`);
    }
    return {
      body: new Uint8Array(await response.arrayBuffer()),
      etag: response.headers.get("etag")?.replace(/^"|"$/g, "") ?? null,
      contentType: response.headers.get("content-type"),
    };
  }

  async putObjectIfAbsent(
    key: string,
    body: Uint8Array,
    contentType = "application/octet-stream",
  ): Promise<S3PageObjectHead> {
    return await this.#putObject(key, body, {
      "content-type": contentType,
      "if-none-match": "*",
    });
  }

  async putObjectIfMatch(
    key: string,
    body: Uint8Array,
    etag: string,
    contentType = "application/octet-stream",
  ): Promise<S3PageObjectHead> {
    return await this.#putObject(key, body, {
      "content-type": contentType,
      "if-match": etag,
    });
  }

  async getHeadRef(): Promise<S3PageObject | null> {
    return await this.getObject("refs/head");
  }

  async putSegment(segmentId: string, body: Uint8Array): Promise<S3PageObjectHead> {
    return await this.putObjectIfAbsent(`segments/${segmentId}`, body);
  }

  async putCommit(commitId: string, body: Uint8Array): Promise<S3PageObjectHead> {
    return await this.putObjectIfAbsent(`commits/${commitId}`, body);
  }

  async casHead(body: Uint8Array, etag: string): Promise<S3PageObjectHead> {
    return await this.putObjectIfMatch("refs/head", body, etag);
  }

  async #putObject(
    key: string,
    body: Uint8Array,
    headers: Record<string, string>,
  ): Promise<S3PageObjectHead> {
    const response = await this.#signedFetch("PUT", key, body, headers);
    if (!response.ok) {
      throw new Error(`S3 PUT ${key} failed: ${response.status}`);
    }
    return {
      etag: response.headers.get("etag")?.replace(/^"|"$/g, "") ?? null,
    };
  }

  async #signedFetch(
    method: "GET" | "HEAD" | "PUT",
    key: string,
    body: Uint8Array | undefined,
    headers: Record<string, string>,
  ): Promise<Response> {
    return await this.#signedFetchToPath(
      method,
      buildObjectPath(this.bucket, this.prefix, key),
      body,
      headers,
    );
  }

  async #signedFetchToPath(
    method: "GET" | "HEAD" | "PUT",
    path: string,
    body: Uint8Array | undefined,
    headers: Record<string, string>,
  ): Promise<Response> {
    const credentials = await this.#credentials();
    const now = this.#clock();
    const amzDate = formatAmzDate(now);
    const dateStamp = formatDateStamp(now);
    const payloadHash = toHex(await sha256(body ?? new Uint8Array()));
    const url = `${this.endpoint}${path}`;

    const signedHeaders = new Headers();
    signedHeaders.set("host", new URL(this.endpoint).host);
    signedHeaders.set("x-amz-content-sha256", payloadHash);
    signedHeaders.set("x-amz-date", amzDate);
    for (const [name, value] of Object.entries(headers)) {
      signedHeaders.set(name, value);
    }
    if (credentials.sessionToken != null) {
      signedHeaders.set("x-amz-security-token", credentials.sessionToken);
    }

    const canonicalHeaders: Array<readonly [string, string]> = [];
    signedHeaders.forEach((value, name) => {
      canonicalHeaders.push([name.toLowerCase(), normalizeHeaderValue(value)] as const);
    });
    canonicalHeaders.sort(([lhs], [rhs]) => lhs.localeCompare(rhs));
    const canonicalHeadersString = canonicalHeaders
      .map(([name, value]) => `${name}:${value}\n`)
      .join("");
    const signedHeaderNames = canonicalHeaders.map(([name]) => name).join(";");
    const canonicalRequest = [
      method,
      path,
      "",
      canonicalHeadersString,
      signedHeaderNames,
      payloadHash,
    ].join("\n");
    const scope = `${dateStamp}/${this.region}/s3/aws4_request`;
    const stringToSign = [
      "AWS4-HMAC-SHA256",
      amzDate,
      scope,
      toHex(await sha256(toUtf8(canonicalRequest))),
    ].join("\n");
    const signingKey = await deriveSigningKey(
      credentials.secretAccessKey,
      dateStamp,
      this.region,
    );
    const signature = toHex(await hmacSha256(signingKey, stringToSign));

    signedHeaders.set(
      "authorization",
      [
        "AWS4-HMAC-SHA256 Credential=",
        `${credentials.accessKeyId}/${scope}, `,
        `SignedHeaders=${signedHeaderNames}, `,
        `Signature=${signature}`,
      ].join(""),
    );

    return await this.#fetchImpl(url, {
      method,
      headers: signedHeaders,
      body: body == null ? undefined : toArrayBufferView(body),
    });
  }
}
