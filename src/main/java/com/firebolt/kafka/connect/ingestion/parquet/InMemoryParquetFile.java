package com.firebolt.kafka.connect.ingestion.parquet;

import java.io.ByteArrayOutputStream;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

/**
 * An {@link OutputFile} backed by an in-memory buffer, so Parquet content can be
 * generated without touching disk and shipped directly over HTTP.
 */
final class InMemoryParquetFile implements OutputFile {

    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

    @Override
    public PositionOutputStream create(long blockSizeHint) {
        return new BufferPositionOutputStream(buffer);
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) {
        buffer.reset();
        return new BufferPositionOutputStream(buffer);
    }

    @Override
    public boolean supportsBlockSize() {
        return false;
    }

    @Override
    public long defaultBlockSize() {
        return 0;
    }

    byte[] toByteArray() {
        return buffer.toByteArray();
    }

    private static final class BufferPositionOutputStream extends PositionOutputStream {
        private final ByteArrayOutputStream delegate;

        BufferPositionOutputStream(ByteArrayOutputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public long getPos() {
            return delegate.size();
        }

        @Override
        public void write(int b) {
            delegate.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) {
            delegate.write(b, off, len);
        }

        @Override
        public void close() {
            // the buffer is read after the writer closes; nothing to release
        }
    }
}
