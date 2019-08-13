package com.bluejeans.bigqueue;

import java.io.Closeable;
import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MappedPage implements Closeable {

    private final static Logger logger = LoggerFactory.getLogger(MappedPage.class);

    private ThreadLocalByteBuffer threadLocalBuffer;
    private volatile boolean dirty = false;
    private volatile boolean closed = false;
    private final String pageFile;
    private final long index;
    private static final Cleaner cleaner = Cleaner.create();

    public MappedPage(final MappedByteBuffer mbb, final String pageFile, final long index) {
        this.threadLocalBuffer = new ThreadLocalByteBuffer(mbb);
        this.pageFile = pageFile;
        this.index = index;
    }

    @Override
    public void close() {
        synchronized (this) {
            if (closed)
                return;

            flush();

            final MappedByteBuffer srcBuf = (MappedByteBuffer) threadLocalBuffer.getSourceBuffer();
            unmap(srcBuf);

            this.threadLocalBuffer = null; // hint GC

            closed = true;
            if (logger.isDebugEnabled())
                logger.debug("Mapped page for " + this.pageFile + " was just unmapped and closed.");
        }
    }

    public void setDirty(final boolean dirty) {
        this.dirty = dirty;
    }

    /**
     * Persist any changes to disk
     */

    public void flush() {
        synchronized (this) {
            if (closed)
                return;
            if (dirty) {
                final MappedByteBuffer srcBuf = (MappedByteBuffer) threadLocalBuffer.getSourceBuffer();
                srcBuf.force(); // flush the changes
                dirty = false;
                if (logger.isDebugEnabled())
                    logger.debug("Mapped page for " + this.pageFile + " was just flushed.");
            }
        }
    }

    /**
     * Get data from a thread local copy of the mapped page buffer
     *
     * @param position start position(relative to the start position of source mapped page buffer) of the thread local buffer
     * @param length the length to fetch
     * @return byte data
     */

    public byte[] getLocal(final int position, final int length) {
        final ByteBuffer buf = this.getLocal(position);
        final byte[] data = new byte[length];
        buf.get(data);
        return data;
    }

    /**
     * Get a thread local copy of the mapped page buffer
     *
     * @param position start position(relative to the start position of source mapped page buffer) of the thread local buffer
     * @return a byte buffer with specific position as start position.
     */

    public ByteBuffer getLocal(final int position) {
        final ByteBuffer buf = this.threadLocalBuffer.get();
        buf.position(position);
        return buf;
    }

    private static void unmap(final MappedByteBuffer buffer) {
        cleaner.register(buffer, new Runnable() {

            @Override
            public void run() {
                // TODO Auto-generated method stub

            }
        }).clean();
    }



    private static class ThreadLocalByteBuffer extends ThreadLocal<ByteBuffer> {
        private final ByteBuffer _src;

        public ThreadLocalByteBuffer(final ByteBuffer src) {
            _src = src;
        }

        public ByteBuffer getSourceBuffer() {
            return _src;
        }

        @Override
        protected synchronized ByteBuffer initialValue() {
            final ByteBuffer dup = _src.duplicate();
            return dup;
        }
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public String toString() {
        return "Mapped page for " + this.pageFile + ", index = " + this.index + ".";
    }

    public String getPageFile() {
        return this.pageFile;
    }

    /**
     * The index of the mapped page
     *
     * @return the index
     */

    public long getPageIndex() {
        return this.index;
    }
}
