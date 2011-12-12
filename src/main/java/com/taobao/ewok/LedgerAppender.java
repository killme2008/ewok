package com.taobao.ewok;

import java.io.IOException;
import java.nio.ByteBuffer;

import java.util.Set;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * A ledger appender
 * 
 * @author boyan
 * 
 */
public class LedgerAppender {
    private LedgerHandle handle;
    private EwokConfiguration conf;
    static final Log log = LogFactory.getLog(LedgerAppender.class);
    private BookKeeper bookKeeper;
    private boolean closed = false;


    public LedgerAppender(BookKeeper bookKeeper, LedgerHandle handle, EwokConfiguration conf) {
        super();
        this.bookKeeper = bookKeeper;
        this.handle = handle;
        this.conf = conf;
    }


    public LedgerHandle getHandle() {
        return handle;
    }

    // Last entry for closing ledger handle normally
    public static final byte[] END = new byte[] { (byte) 0x80, (byte) 0x81 };


    public synchronized void close() throws BKException, InterruptedException {
        if (this.handle != null && !closed) {
            // Write END sync to ensure all async added entries to be stored.
            this.handle.addEntry(END);
            this.handle.close();
            closed = true;
        }
    }


    /**
     * Open a read-only LedgerHandle for reading log in no recovery mode
     * 
     * @return
     * @throws BKException
     * @throws InterruptedException
     */
    public synchronized LedgerCursor getCursor() throws BKException, InterruptedException {
        if (closed) {
            LedgerHandle lh =
                    bookKeeper.openLedgerNoRecovery(handle.getId(), DigestType.CRC32, conf.getPassword().getBytes());
            long last = lh.getLastAddConfirmed();
            return new LedgerCursor(last, conf.getCursorBatchSize(), lh);
        }
        else {
            return new LedgerCursor(this.handle.getLastAddConfirmed(), conf.getCursorBatchSize(), handle);
        }
    }

    private static class SyncAddCallback implements AddCallback {
        /**
         * Implementation of callback interface for synchronous read method.
         * 
         * @param rc
         *            return code
         * @param leder
         *            ledger identifier
         * @param entry
         *            entry identifier
         * @param ctx
         *            control object
         */
        public void addComplete(int rc, LedgerHandle lh, long entry, Object ctx) {
            SyncCounter counter = (SyncCounter) ctx;

            counter.setrc(rc);
            counter.dec();
        }
    }

    private ThreadLocal<SyncCounter> lastCounter = new ThreadLocal<SyncCounter>();


    public void force() throws IOException {
        SyncCounter counter = lastCounter.get();
        if (counter != null) {
            try {
                counter.block(0);
                if (counter.getrc() != BKException.Code.OK) {
                    throw new IOException("Force append failed", BKException.create(counter.getrc()));
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
            finally {
                lastCounter.remove();
            }

        }
    }


    /**
     * 写入事务日志
     * 
     * @param tlog
     * @return
     * @throws BKException
     * @throws InterruptedException
     */
    public SyncCounter writeLog(TransactionLogRecord tlog) throws InterruptedException {
        int recordSize = tlog.calculateTotalRecordSize();
        long futureFilePosition = handle.getLength() + recordSize;
        if (futureFilePosition >= conf.getBtmConf().getMaxLogSizeInMb() * 1024 * 1024) {
            if (log.isDebugEnabled())
                log.debug("log file is full (size would be: " + futureFilePosition + ", max allowed: "
                        + conf.getBtmConf().getMaxLogSizeInMb() + "Mbs");
            return null;
        }

        ByteBuffer buf = ByteBuffer.allocate(recordSize);
        buf.putInt(tlog.getStatus());
        buf.putInt(tlog.getRecordLength());
        buf.putInt(tlog.getHeaderLength());
        buf.putLong(tlog.getTime());
        buf.putInt(tlog.getSequenceNumber());
        buf.putInt(tlog.getCrc32());
        buf.put((byte) tlog.getGtrid().getArray().length);
        buf.put(tlog.getGtrid().getArray());
        Set<String> uniqueNames = tlog.getUniqueNames();
        buf.putInt(uniqueNames.size());
        for (String uniqueName : uniqueNames) {
            buf.putShort((short) uniqueName.length());
            buf.put(uniqueName.getBytes());
        }
        buf.putInt(tlog.getEndRecord());

        byte[] data = buf.array();
        SyncCounter counter = new SyncCounter();
        counter.inc();
        handle.asyncAddEntry(data, new SyncAddCallback(), counter);
        lastCounter.set(counter);
        return counter;
    }
}
