package com.taobao.ewok;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicReference;

import javax.transaction.Status;

import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;

import bitronix.tm.TransactionManagerServices;
import bitronix.tm.journal.CorruptedTransactionLogException;
import bitronix.tm.journal.Journal;
import bitronix.tm.utils.Decoder;
import bitronix.tm.utils.InitializationException;
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
    private AtomicReference<FutureTask<LedgerAppender>> activeApd;
    static final Log log = LogFactory.getLog(BookkeeperJournal.class);
    private Set<HandleState> handles = new HashSet<HandleState>();
    private volatile boolean opened = false;


    EwokZookeeper getEwokZookeeper() {
        return ewokZookeeper;
    }


    public BookkeeperJournal() {
        conf = new EwokConfiguration();
        initZookeeper();
        initBookkeeper();
    }


    private void initBookkeeper() {
        try {
            ClientConfiguration clientConf =
                    new ClientConfiguration().setZkServers(conf.getZkServers())
                        .setZkTimeout(conf.getZkSessionTimeout());
            this.bookKeeper = new BookKeeper(clientConf);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InitializationException("Init bookkeeper intrrupted", e);
        }
        catch (Exception e) {
            throw new InitializationException("Error creating BookKeeper", e);
        }
    }


    private void initZookeeper() {
        try {
            this.ewokZookeeper = new EwokZookeeper(conf);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        catch (Exception e) {
            throw new InitializationException("Error creating EwokZookeeper", e);
        }
    }


    public void shutdown() {
        try {
            this.clone();
        }
        catch (Throwable e) {
            log.error("error shutting down bookkeeper journal. Transaction log integrity could be compromised!", e);
        }

    }


    public synchronized void close() throws IOException {
        try {
            this.force();
            if (this.activeApd != null) {
                try {
                    getCurrentAppender().close();
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
            LedgerAppender currentAppender = getCurrentAppender();
            if (currentAppender == null) {
                return Collections.EMPTY_MAP;
            }
            Map<Uid, TransactionLogRecord> map = collectDanglingRecords(currentAppender);
            if (map == null || map.isEmpty())
                return Collections.EMPTY_MAP;
            Map<Uid, bitronix.tm.journal.TransactionLogRecord> rt =
                    new HashMap<Uid, bitronix.tm.journal.TransactionLogRecord>();
            long minEntryId = -1;
            for (Map.Entry<Uid, TransactionLogRecord> entry : map.entrySet()) {
                TransactionLogRecord record = entry.getValue();
                rt.put(
                    entry.getKey(),
                    new bitronix.tm.journal.TransactionLogRecord(record.getStatus(), record.getRecordLength(), record
                        .getHeaderLength(), record.getTime(), record.getSequenceNumber(), record.getCrc32(), record
                        .getGtrid(), record.getUniqueNames(), record.getEndRecord()));
                if (record.getEntryId() < minEntryId || minEntryId == -1) {
                    minEntryId = record.getEntryId();
                }
            }
            HandleState state = currentAppender.getState();
            if (minEntryId > 0 && minEntryId > state.checkpoint) {
                // Update checkpoint
                state.checkpoint = minEntryId;
                try {
                    this.ewokZookeeper.writeHandles(this.handles);
                }
                catch (KeeperException e) {
                    log.error("Update checkpoint failed", e);
                }
            }
            return rt;
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
        getCurrentAppender().force();
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
        synchronized (this) {
            while (!opened)
                try {
                    this.wait();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException(e);
                }
        }
        FutureTask<LedgerAppender> currAppenderTask = this.activeApd.get();
        if (currAppenderTask == null)
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
            while (true) {
                final LedgerAppender currentAppender = currAppenderTask.get();
                SyncCounter written = currentAppender.writeLog(tlog);
                if (written == null) {
                    FutureTask<LedgerAppender> newTask = new FutureTask<LedgerAppender>(new Callable<LedgerAppender>() {
                        public LedgerAppender call() throws Exception {
                            return swapJournalFiles(currentAppender);
                        }

                    });
                    if (this.activeApd.compareAndSet(currAppenderTask, newTask)) {
                        newTask.run();
                    }
                    currAppenderTask = this.activeApd.get();
                }
                else
                    return;
            }
        }
        catch (ExecutionException e) {
            throw new IOException("Could not write log to bookkeeper", e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
        catch (Exception e) {
            // TODO create a new ledger?
            throw new IOException("Could not write log to bookkeeper", e);
        }

    }

    static final int MAX_RETRY_COUNT = 3;


    LedgerAppender swapJournalFiles(LedgerAppender oldAppender) throws InterruptedException, BKException,
            KeeperException, IOException {
        oldAppender.force();
        LedgerAppender passiveApd = null;
        for (int i = 0; i < MAX_RETRY_COUNT; i++) {
            try {
                passiveApd = createNewLedgerAppender(bookKeeper, conf);
                if (passiveApd != null)
                    break;
            }
            catch (BKException e) {
                log.error("Create new ledger failed", e);
            }
        }
        if (passiveApd == null)
            throw new IOException("Could not create a new ledger");
        copyDanglingRecords(oldAppender, passiveApd);
        boolean removed = this.handles.remove(oldAppender.getState());
        assert (removed);
        boolean added = this.handles.add(passiveApd.getState());
        assert (added);
        for (int i = 0; i < MAX_RETRY_COUNT; i++) {
            try {
                this.ewokZookeeper.writeHandles(handles);
                break;
            }
            catch (KeeperException e) {
                log.error("Write ids to zookeeper failed", e);
                if (i == MAX_RETRY_COUNT - 1)
                    throw e;
            }
        }
        oldAppender.close();
        long oldId = oldAppender.getHandle().getId();
        deleteLedger(oldId);
        return passiveApd;
    }


    /**
     * Open a read-only LedgerHandle for reading log in recover mode
     * 
     * @return
     * @throws BKException
     * @throws InterruptedException
     */
    LedgerCursor getCursor(HandleState state) throws BKException, InterruptedException {
        try {
            LedgerHandle lh = bookKeeper.openLedger(state.id, DigestType.CRC32, conf.getPassword().getBytes());
            long last = lh.getLastAddConfirmed();
            return new LedgerCursor(state.checkpoint, last, conf.getCursorBatchSize(), lh);
        }
        catch (BKException e) {
            if (e.getCode() == BKException.Code.NoSuchLedgerExistsException) {
                return null;
            }
            else
                throw e;
        }
    }


    EwokConfiguration getConf() {
        return conf;
    }


    Set<HandleState> getHandles() {
        return handles;
    }


    public synchronized void open() throws IOException {
        try {
            this.activeApd = new AtomicReference<FutureTask<LedgerAppender>>();
            FutureTask<LedgerAppender> task = new FutureTask<LedgerAppender>(new Callable<LedgerAppender>() {
                public LedgerAppender call() throws Exception {
                    return createNewLedgerAppender(bookKeeper, conf);
                }

            });
            // Run it right now.
            task.run();
            this.activeApd.set(task);
            Set<HandleState> existsHandles = this.ewokZookeeper.readHandles();
            Set<HandleState> loadHandles = this.ewokZookeeper.readHandles(conf.getLoadZkPath());
            Set<HandleState> uniqSet = new HashSet<HandleState>(existsHandles);
            uniqSet.addAll(loadHandles);
            if (existsHandles.isEmpty() && loadHandles.isEmpty()) {
                // No log history
            }
            else {
                List<HandleState> sortedHandles = new ArrayList<HandleState>(uniqSet);
                Collections.sort(sortedHandles);
                for (HandleState state : sortedHandles) {
                    LedgerCursor cursor = getCursor(state);
                    if (cursor != null) {
                        try {
                            copyDanglingRecords(cursor, getCurrentAppender());
                            this.force();
                        }
                        finally {
                            cursor.close();
                        }
                    }

                }
            }
            // Copy all dangling records done,then store the new handle to zk
            this.handles.add(getCurrentAppender().getState());
            this.ewokZookeeper.writeHandles(this.handles);

            // Now it's safe to delete old ledgers

            for (final HandleState state : uniqSet) {
                deleteLedger(state.id);
            }
            this.opened = true;
            this.notifyAll();
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


    LedgerAppender getCurrentAppender() {
        try {
            return this.activeApd.get().get();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Get current appender failed", e);
        }
        catch (ExecutionException e) {
            throw new IllegalStateException("Get current appender failed");
        }
    }


    void deleteLedger(final long id) {
        bookKeeper.asyncDeleteLedger(id, new AsyncCallback.DeleteCallback() {

            public void deleteComplete(int rc, Object ctx) {
                if (rc != BKException.Code.OK) {
                    log.error("Delete ledger(" + id + ") failed,response:" + rc);
                }
                else {
                    log.warn("Delete ledger(" + id + " successfully");
                }

            }
        }, null);
    }


    static LedgerAppender createNewLedgerAppender(BookKeeper bookKeeper, EwokConfiguration conf)
            throws InterruptedException, BKException {
        LedgerHandle lh =
                bookKeeper.createLedger(conf.getESize(), conf.getQSize(), DigestType.CRC32, conf.getPassword()
                    .getBytes());
        log.warn("Create a new ledger:" + lh.getId());
        return new LedgerAppender(bookKeeper, lh, new HandleState(lh.getId(), 0), conf);
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
        LedgerCursor tlc = tla.getCursor(tla.getState().checkpoint);
        if (tlc != null)
            return collectDanglingRecords(tlc);
        else
            return Collections.emptyMap();
    }


    private static Map<Uid, TransactionLogRecord> collectDanglingRecords(LedgerCursor tlc) throws IOException,
            InterruptedException, CorruptedTransactionLogException {
        Map<Uid, TransactionLogRecord> danglingRecords = new HashMap<Uid, TransactionLogRecord>(64);

        int committing = 0;
        int committed = 0;

        while (true) {
            TransactionLogRecord tlog = null;
            try {
                tlog = tlc.readLog();
            }
            catch (BKException ex) {
                if (TransactionManagerServices.getConfiguration().isSkipCorruptedLogs()) {
                    log.error("skipping corrupted log", ex);
                    continue;
                }
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
                        danglingRecords.put(tlog.getGtrid(), new TransactionLogRecord(rec.getStatus(), rec.getGtrid(),
                            recUniqueNames));
                    }
                }
            }
        }

        if (log.isDebugEnabled())
            log.debug("collected dangling records of " + tlc.getHandle().getId() + ", committing: " + committing
                    + ", committed: " + committed + ", delta: " + danglingRecords.size());

        return danglingRecords;
    }

}
