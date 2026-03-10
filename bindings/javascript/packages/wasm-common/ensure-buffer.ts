import { Buffer as NodeBuffer } from "buffer";

type GlobalWithBuffer = typeof globalThis & {
    Buffer?: typeof NodeBuffer;
};

const globalWithBuffer = globalThis as GlobalWithBuffer;

if (globalWithBuffer.Buffer == null) {
    globalWithBuffer.Buffer = NodeBuffer;
}
