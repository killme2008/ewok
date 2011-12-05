package com.taobao.ewok;

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


    public synchronized void close() throws BKException, InterruptedException {
        if (this.handle != null && !closed) {
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


    /**
     * 写入事务日志
     * 
     * @param tlog
     * @return
     * @throws BKException
     * @throws InterruptedException
     */
    public boolean writeLog(TransactionLogRecord tlog) throws InterruptedException {
        int recordSize = tlog.calculateTotalRecordSize();
        long futureFilePosition = handle.getLength() + recordSize;
        if (futureFilePosition >= conf.getBtmConf().getMaxLogSizeInMb() * 1024 * 1024) {
            if (log.isDebugEnabled())
                log.debug("log file is full (size would be: " + futureFilePosition + ", max allowed: "
                        + conf.getBtmConf().getMaxLogSizeInMb() + "Mbs");
            return false;
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
        try {
            handle.addEntry(data);
        }
        catch (BKException e) {
            log.error("Write entry to bookkeeper failed", e);
            return false;
        }
        return true;
    }
}
