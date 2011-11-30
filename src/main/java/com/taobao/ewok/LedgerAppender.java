package com.taobao.ewok;

import java.nio.ByteBuffer;

import java.util.Set;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
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


    public LedgerAppender(BookKeeper bookKeeper, LedgerHandle handle, EwokConfiguration conf) {
        super();
        this.bookKeeper = bookKeeper;
        this.handle = handle;
        this.conf = conf;
    }


    public LedgerHandle getHandle() {
        return handle;
    }


    public void close() throws BKException, InterruptedException {
        this.handle.close();
    }


    /**
     * Open a read-only LedgerHandle for reading log
     * 
     * @return
     * @throws BKException
     * @throws InterruptedException
     */
    public LedgerCursor getCursor() throws BKException, InterruptedException {
        LedgerHandle lh =
                bookKeeper.openLedgerNoRecovery(handle.getId(), DigestType.CRC32, conf.getPassword().getBytes());
        long last = lh.getLastAddConfirmed();
        return new LedgerCursor(last, conf.getCursorBatchSize(), lh);
    }


    /**
     * 写入事务日志
     * 
     * @param tlog
     * @return
     * @throws BKException
     * @throws InterruptedException
     */
    public boolean writeLog(TransactionLogRecord tlog) throws BKException, InterruptedException {
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

        buf.flip();

        // 暂时都使用同步写入，TODO
        handle.addEntry(buf.array());

        return true;
    }
}
