package com.taobao.ewok;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.transaction.Status;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;

import bitronix.tm.TransactionManagerServices;
import bitronix.tm.journal.CorruptedTransactionLogException;
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
    private LedgerAppender activeApd;
    static final Log log = LogFactory.getLog(BookkeeperJournal.class);
    private Set<Long> handles = new HashSet<Long>();


    public BookkeeperJournal() {

    }


    public void shutdown() {
        // TODO Auto-generated method stub

    }


    public synchronized void close() throws IOException {
        try {
            if (this.activeApd != null) {
                try {
                    this.activeApd.close();
                    this.activeApd = null;
                }
                catch (BKException e) {
                    log.error("Close handle failed", e);
                }
            }

            if (this.bookKeeper != null) {
                try {
                    this.bookKeeper.close();
                }
                catch (BKException e) {
                    log.error("Close bookKeeper failed", e);
                }
            }

            if (this.ewokZookeeper != null) {
                ewokZookeeper.close();
            }
            this.handles.clear();

        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

    }


    public Map collectDanglingRecords() throws IOException {
        try {
            return collectDanglingRecords(this.activeApd);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
        catch (BKException e) {
            throw new IOException("Collect dangling records from bookkeeper failed", e);
        }
    }


    public void force() throws IOException {
        // ignore
        // Store entries in forcing way by default.
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
        if (activeApd == null)
            throw new IOException("cannot write log, bookkeeper logger is not open");

        if (conf.getBtmConf().isFilterLogStatus()) {
            if (status != Status.STATUS_COMMITTING && status != Status.STATUS_COMMITTED
                    && status != Status.STATUS_UNKNOWN) {
                if (log.isDebugEnabled())
                    log.debug("filtered out write to log for status " + Decoder.decodeStatus(status));
                return;
            }
        }

        TransactionLogRecord tlog = new TransactionLogRecord(status, gtrid, uniqueNames);
        try {
            synchronized (this) {
                boolean written = activeApd.writeLog(tlog);
                if (!written) {
                    // time to swap log files
                    swapJournalFiles();

                    written = activeApd.writeLog(tlog);
                    if (!written)
                        throw new IOException(
                            "Could not write log to journal even after swap, circular collision avoided");
                }
            }
        }
        catch (KeeperException e) {
            throw new IOException("Could not write log to bookkeeper", e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
        catch (BKException e) {
            throw new IOException("Could not write log to bookkeeper", e);
        }

    }


    private void swapJournalFiles() throws InterruptedException, BKException, KeeperException, IOException {
        LedgerAppender passiveApd = createNewLedgerAppender();
        copyDanglingRecords(activeApd, passiveApd);

        this.handles.remove(activeApd.getHandle().getId());
        this.handles.add(passiveApd.getHandle().getId());
        this.ewokZookeeper.writeLogIds(handles);

        activeApd.close();
        activeApd = passiveApd;
    }


    /**
     * Open a read-only LedgerHandle for reading log
     * 
     * @return
     * @throws BKException
     * @throws InterruptedException
     */
    public LedgerCursor getCursor(long id) throws BKException, InterruptedException {
        LedgerHandle lh = bookKeeper.openLedgerNoRecovery(id, DigestType.CRC32, conf.getPassword().getBytes());
        long last = lh.getLastAddConfirmed();
        return new LedgerCursor(last, conf.getCursorBatchSize(), lh);
    }


    public synchronized void open() throws IOException {
        try {
            this.activeApd = createNewLedgerAppender();
            Set<Long> existsIds = this.ewokZookeeper.readLogIds();
            if (existsIds.isEmpty()) {
                // No log history
            }
            else {
                List<Long> sortedIds = new ArrayList<Long>(existsIds);
                Collections.sort(sortedIds);
                for (long id : sortedIds) {
                    LedgerCursor cursor = getCursor(id);
                    copyDanglingRecords(cursor, this.activeApd);
                }
            }
            // Copy all dangling records done,then store new handels to zk
            this.handles.add(this.activeApd.getHandle().getId());
            this.ewokZookeeper.writeLogIds(this.handles);
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


    private LedgerAppender createNewLedgerAppender() throws InterruptedException, BKException {
        LedgerHandle lh =
                bookKeeper.createLedger(conf.getESize(), conf.getQSize(), DigestType.CRC32, conf.getPassword()
                    .getBytes());
        return new LedgerAppender(bookKeeper, lh, conf);
    }


    /**
     * Copy all records that have status COMMITTING and no corresponding
     * COMMITTED record from the fromTla to the toTla.
     * 
     * @param fromTla
     *            the source where to search for COMMITTING records with no
     *            corresponding COMMITTED record
     * @param toTla
     *            the destination where the COMMITTING records will be copied to
     * @throws java.io.IOException
     *             in case of disk IO failure.
     */
    private static void copyDanglingRecords(LedgerAppender fromTla, LedgerAppender toTla) throws IOException,
            InterruptedException, BKException {
        if (log.isDebugEnabled())
            log.debug("starting copy of dangling records");

        Map<Uid, TransactionLogRecord> danglingRecords = collectDanglingRecords(fromTla);
        for (TransactionLogRecord tlog : danglingRecords.values()) {
            toTla.writeLog(tlog);
        }

        if (log.isDebugEnabled())
            log.debug(danglingRecords.size() + " dangling record(s) copied to passive log file");
    }


    private static void copyDanglingRecords(LedgerCursor cursor, LedgerAppender toTla) throws IOException,
            InterruptedException, BKException {
        if (log.isDebugEnabled())
            log.debug("starting copy of dangling records");

        Map<Uid, TransactionLogRecord> danglingRecords = collectDanglingRecords(cursor);
        for (TransactionLogRecord tlog : danglingRecords.values()) {
            toTla.writeLog(tlog);
        }

        if (log.isDebugEnabled())
            log.debug(danglingRecords.size() + " dangling record(s) copied to passive log file");
    }


    /**
     * Create a Map of TransactionLogRecord with COMMITTING status objects using
     * the GTRID byte[] as key that have no corresponding COMMITTED record
     * 
     * @param tla
     *            the TransactionLogAppender to scan
     * @return a Map using Uid objects GTRID as key and
     *         {@link TransactionLogRecord} as value
     * @throws java.io.IOException
     *             in case of disk IO failure.
     */
    private static Map<Uid, TransactionLogRecord> collectDanglingRecords(LedgerAppender tla) throws IOException,
            BKException, InterruptedException {

        LedgerCursor tlc = tla.getCursor();
        return collectDanglingRecords(tlc);
    }


    private static Map<Uid, TransactionLogRecord> collectDanglingRecords(LedgerCursor tlc) throws IOException,
            BKException, InterruptedException, CorruptedTransactionLogException {
        Map<Uid, TransactionLogRecord> danglingRecords = new HashMap<Uid, TransactionLogRecord>(64);
        try {
            int committing = 0;
            int committed = 0;

            while (true) {
                TransactionLogRecord tlog;
                try {
                    tlog = tlc.readLog();
                }
                catch (CorruptedTransactionLogException ex) {
                    if (TransactionManagerServices.getConfiguration().isSkipCorruptedLogs()) {
                        log.error("skipping corrupted log", ex);
                        continue;
                    }
                    throw ex;
                }

                if (tlog == null)
                    break;

                int status = tlog.getStatus();
                if (status == Status.STATUS_COMMITTING) {
                    danglingRecords.put(tlog.getGtrid(), tlog);
                    committing++;
                }
                if (status == Status.STATUS_COMMITTED || status == Status.STATUS_UNKNOWN) {
                    TransactionLogRecord rec = danglingRecords.get(tlog.getGtrid());
                    if (rec != null) {
                        Set<String> recUniqueNames = new HashSet<String>(rec.getUniqueNames());
                        recUniqueNames.removeAll(tlog.getUniqueNames());
                        if (recUniqueNames.isEmpty()) {
                            danglingRecords.remove(tlog.getGtrid());
                            committed++;
                        }
                        else {
                            danglingRecords.put(tlog.getGtrid(),
                                new TransactionLogRecord(rec.getStatus(), rec.getGtrid(), recUniqueNames));
                        }
                    }
                }
            }

            if (log.isDebugEnabled())
                log.debug("collected dangling records of " + tlc.getHandle().getId() + ", committing: " + committing
                        + ", committed: " + committed + ", delta: " + danglingRecords.size());
        }
        finally {
            tlc.close();
        }
        return danglingRecords;
    }

}
