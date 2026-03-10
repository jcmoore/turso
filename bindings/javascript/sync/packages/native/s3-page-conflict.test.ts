import { expect, test } from "vitest";

import { SyncConflictError, isS3PageSyncConflictLike } from "./promise.js";

test("recognizes stale snapshot errors as sync conflicts", () => {
  expect(
    isS3PageSyncConflictLike(
      new Error(
        "step failed: Database snapshot is stale. You must rollback and retry the whole transaction.",
      ),
    ),
  ).toBe(true);
});

test("wraps stale snapshot failures in SyncConflictError", () => {
  const err = new SyncConflictError(
    "step failed: Database snapshot is stale. You must rollback and retry the whole transaction.",
  );
  expect(err.code).toBe("SYNC_CONFLICT");
  expect(err.name).toBe("SyncConflictError");
});
