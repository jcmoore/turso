import path from "node:path";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const emnapiEntry = require.resolve("emnapi");

process.stdout.write(
  path.join(path.dirname(emnapiEntry), "lib", "wasm32-wasi-threads"),
);
