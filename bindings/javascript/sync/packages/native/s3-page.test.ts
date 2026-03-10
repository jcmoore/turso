import { describe, expect, it, vi } from "vitest";
import { S3PageClient } from "../common/s3-page.js";

describe("S3PageClient", () => {
  it("uses immutable writes for segments", async () => {
    const fetchImpl = vi.fn(async (_input: RequestInfo | URL, init?: RequestInit) => {
      return new Response(null, {
        status: 200,
        headers: { etag: '"segment-etag"' },
      });
    });
    const client = new S3PageClient(
      {
        backend: "s3_page_v1",
        endpoint: "http://localhost:8333/",
        bucket: "suite-bucket",
        prefix: "db/prefix",
        region: "us-east-2",
        credentials: async () => ({
          accessKeyId: "AKIDEXAMPLE",
          secretAccessKey: "secret",
          sessionToken: "session-token",
        }),
        remoteEncryption: {
          key: "base64-key",
          cipher: "aegis256",
        },
      },
      {
        fetchImpl,
        clock: () => new Date("2026-03-10T12:34:56Z"),
      },
    );

    const result = await client.putSegment("segment-1", Uint8Array.of(1, 2, 3));

    expect(result).toEqual({ etag: "segment-etag" });
    expect(fetchImpl).toHaveBeenCalledTimes(1);
    const [url, init] = fetchImpl.mock.calls[0]!;
    expect(url).toBe("http://localhost:8333/suite-bucket/db/prefix/segments/segment-1");
    expect(init?.method).toBe("PUT");
    const headers = new Headers(init?.headers);
    expect(headers.get("if-none-match")).toBe("*");
    expect(headers.get("x-amz-security-token")).toBe("session-token");
    expect(headers.get("authorization")).toContain(
      "Credential=AKIDEXAMPLE/20260310/us-east-2/s3/aws4_request",
    );
  });

  it("uses CAS writes for refs/head", async () => {
    const fetchImpl = vi.fn(async () => {
      return new Response(null, {
        status: 200,
        headers: { etag: '"next-head"' },
      });
    });
    const client = new S3PageClient(
      {
        backend: "s3_page_v1",
        endpoint: "http://localhost:8333",
        bucket: "suite-bucket",
        prefix: "",
        credentials: async () => ({
          accessKeyId: "AKIDEXAMPLE",
          secretAccessKey: "secret",
        }),
        remoteEncryption: {
          key: "base64-key",
          cipher: "aegis256",
        },
      },
      {
        fetchImpl,
        clock: () => new Date("2026-03-10T00:00:00Z"),
      },
    );

    const result = await client.casHead(Uint8Array.of(9, 9), "cached-etag");

    expect(result).toEqual({ etag: "next-head" });
    const [url, init] = fetchImpl.mock.calls[0]!;
    expect(url).toBe("http://localhost:8333/suite-bucket/refs/head");
    const headers = new Headers(init?.headers);
    expect(headers.get("if-match")).toBe("cached-etag");
  });

  it("creates buckets on the bucket root path", async () => {
    const fetchImpl = vi.fn(async () => new Response(null, { status: 200 }));
    const client = new S3PageClient(
      {
        backend: "s3_page_v1",
        endpoint: "http://localhost:8333",
        bucket: "suite-bucket",
        prefix: "ignored-prefix",
        credentials: async () => ({
          accessKeyId: "AKIDEXAMPLE",
          secretAccessKey: "secret",
        }),
        remoteEncryption: {
          key: "base64-key",
          cipher: "aegis256",
        },
      },
      {
        fetchImpl,
        clock: () => new Date("2026-03-10T00:00:00Z"),
      },
    );

    await expect(client.createBucket()).resolves.toBeUndefined();

    const [url, init] = fetchImpl.mock.calls[0]!;
    expect(url).toBe("http://localhost:8333/suite-bucket");
    expect(init?.method).toBe("PUT");
  });

  it("parses HEAD etags and handles 404s", async () => {
    const fetchImpl = vi
      .fn<(
        input: RequestInfo | URL,
        init?: RequestInit,
      ) => Promise<Response>>()
      .mockResolvedValueOnce(
        new Response(null, {
          status: 200,
          headers: { etag: '"head-etag"' },
        }),
      )
      .mockResolvedValueOnce(new Response(null, { status: 404 }));
    const client = new S3PageClient(
      {
        backend: "s3_page_v1",
        endpoint: "http://localhost:8333",
        bucket: "suite-bucket",
        prefix: "root",
        credentials: async () => ({
          accessKeyId: "AKIDEXAMPLE",
          secretAccessKey: "secret",
        }),
        remoteEncryption: {
          key: "base64-key",
          cipher: "aegis256",
        },
      },
      { fetchImpl },
    );

    await expect(client.headObject("refs/head")).resolves.toEqual({ etag: "head-etag" });
    await expect(client.headObject("commits/missing")).resolves.toBeNull();
  });
});
