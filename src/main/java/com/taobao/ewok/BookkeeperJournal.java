package com.taobao.ewok;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

import javax.transaction.Status;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;

import bitronix.tm.Configuration;
import bitronix.tm.TransactionManagerServices;
import bitronix.tm.journal.Journal;
import bitronix.tm.utils.Decoder;
import bitronix.tm.utils.Uid;


/**
 * Use bookkeeper as BTM's transaction logs journal
 * 
 * @author boyan(boyan@taobao.com)
 * 
 */
public class BookkeeperJournal implements Journal {
    private EwokZookeeper ewokZookeeper;
    private BookKeeper bookKeeper;
    private EwokConfiguration conf;
    private LedgerHandle currHandle;
    private Configuration btmConf;
    static final Log log = LogFactory.getLog(BookkeeperJournal.class);


    public BookkeeperJournal() {

    }


    public void shutdown() {
        // TODO Auto-generated method stub

    }


    public void close() throws IOException {
        // TODO Auto-generated method stub

    }


    public Map collectDanglingRecords() throws IOException {
        // TODO Auto-generated method stub
        return null;
    }


    public void force() throws IOException {
        // ignore
        // Bookkeeper store entry in force way by default.
    }


    /**
     * Log a new transaction status to journal. Note that the BookkeeperJournal
     * will not check the flow of the transaction. If you call this method with
     * erroneous data, it will be added to the journal anyway.
     * 
     * @param status
     *            transaction status to log. See
     *            {@link javax.transaction.Status} constants.
     * @param gtrid
     *            raw GTRID of the transaction.
     * @param uniqueNames
     *            unique names of the
     *            {@link bitronix.tm.resource.common.ResourceBean}s
     *            participating in this transaction.
     * @throws java.io.IOException
     *             in case of disk IO failure or if the disk journal is not
     *             open.
     */
    public void log(int status, Uid gtrid, Set uniqueNames) throws IOException {
        if (TransactionManagerServices.getConfiguration().isFilterLogStatus()) {
            if (status != Status.STATUS_COMMITTING && status != Status.STATUS_COMMITTED
                    && status != Status.STATUS_UNKNOWN) {
                if (log.isDebugEnabled())
                    log.debug("filtered out write to log for status " + Decoder.decodeStatus(status));
                return;
            }
        }

        TransactionLogRecord tlog = new TransactionLogRecord(status, gtrid, uniqueNames);
        synchronized (this) {
            // boolean written = activeTla.writeLog(tlog);
            // if (!written) {
            // // time to swap log files
            // swapJournalFiles();
            //
            // written = activeTla.writeLog(tlog);
            // if (!written)
            // throw new
            // IOException("no room to write log to journal even after swap, circular collision avoided");
            // }
        } // synchro

    }


    private boolean addEntry(TransactionLogRecord tlog) {

        int recordSize = tlog.calculateTotalRecordSize();
        long futureFilePosition = currHandle.getLength() + recordSize;
        if (futureFilePosition >= btmConf.getMaxLogSizeInMb() * 1024 * 1024) {
            if (log.isDebugEnabled())
                log.debug("log file is full (size would be: " + futureFilePosition + ", max allowed: "
                        + btmConf.getMaxLogSizeInMb() + "Mb)");
            return false;
        }
        if (log.isDebugEnabled())
            log.debug("between " + getHeader().getPosition() + " and " + futureFilePosition + ", writing " + tlog);

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
        while (buf.hasRemaining()) {
            this.fc.write(buf);
        }
    }


    public synchronized void open() throws IOException {
        try {
            long id = this.ewokZookeeper.readLogId();
            LedgerHandle lh = null;
            if (id == -1) {
                // Create legder
                lh = createNewLedger();
            }
            else {
                lh = bookKeeper.openLedger(id, DigestType.CRC32, conf.getPassword().getBytes());
            }
            this.currHandle = lh;
        }
        catch (KeeperException e) {
            throw new IOException(e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
        catch (BKException e) {
            throw new IOException(e);
        }
    }


    private LedgerHandle createNewLedger() throws InterruptedException, BKException, KeeperException {
        LedgerHandle lh;
        lh = bookKeeper.createLedger(conf.getESize(), conf.getQSize(), DigestType.CRC32, conf.getPassword().getBytes());
        long newId = lh.getId();
        // Write it to zookeeper
        this.ewokZookeeper.writeLogId(newId);
        return lh;
    }

}
