package uk.ac.ntu.cloudfs.lb.core;

import java.io.IOException;
import java.io.InputStream;

public final class Chunker {
    private Chunker() {}

    /** Reads up to chunkSize bytes into a new byte[]. Returns null on EOF. */
    public static byte[] nextChunk(InputStream in, int chunkSize) throws IOException {
        byte[] buf = new byte[chunkSize];
        int off = 0;
        while (off < chunkSize) {
            int r = in.read(buf, off, chunkSize - off);
            if (r == -1) break;
            off += r;
        }
        if (off == 0) return null;
        if (off == chunkSize) return buf;

        byte[] exact = new byte[off];
        System.arraycopy(buf, 0, exact, 0, off);
        return exact;
    }
}