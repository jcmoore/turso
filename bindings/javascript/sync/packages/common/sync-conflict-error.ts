export class SyncConflictError extends Error {
  code: string;
  cause: unknown;

  constructor(message = "Sync conflict", cause?: unknown) {
    super(message);
    this.name = "SyncConflictError";
    this.code = "SYNC_CONFLICT";
    this.cause = cause;

    (Error as any).captureStackTrace?.(this, SyncConflictError);
  }
}

export function isSyncConflictError(error: unknown): boolean {
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
