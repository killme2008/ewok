package com.taobao.ewok;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;

import bitronix.tm.journal.CorruptedTransactionLogException;
import bitronix.tm.utils.Uid;


/**
 * Ledger log cursor
 * 
 * @author boyan(boyan@taobao.com)
 * 
 */
public class LedgerCursor {
    private final long lastEntry;
    private final LedgerHandle handle;
    private long startEntry;
    private long endEntry;
    private int batchSize = 5;
    private Enumeration<LedgerEntry> iterator;


    public LedgerCursor(final long startEntry, final long lastEntry, final int batchSize, final LedgerHandle handle) {
        super();
        this.startEntry = startEntry;
        this.lastEntry = lastEntry;
        this.batchSize = batchSize;
        this.handle = handle;
    }


    public LedgerHandle getHandle() {
        return this.handle;
    }


    public void close() throws BKException, InterruptedException {
        this.handle.close();
    }


    public TransactionLogRecord readLog() throws IOException, BKException, InterruptedException {
        return this.readLog(false);
    }


    Enumeration<LedgerEntry> getIterator() {
        return this.iterator;
    }


    public synchronized TransactionLogRecord readLog(final boolean skipCrcCheck) throws IOException, BKException,
            InterruptedException {
        while (this.iterator != null && this.iterator.hasMoreElements()) {
            final LedgerEntry entry = this.iterator.nextElement();
            if (entry != null) {
                final DataInputStream in = new DataInputStream(entry.getEntryInputStream());
                if (in.available() > 4) {
                    final int status = in.readInt();
                    final int recordLen = in.readInt();
                    final int headerLen = in.readInt();
                    final long time = in.readLong();
                    final int seqNo = in.readInt();
                    final int crc32 = in.readInt();
                    final byte gtridLen = in.readByte();
                    final byte[] gtridArray = new byte[gtridLen];
                    in.read(gtridArray);
                    final Uid gtrid = new Uid(gtridArray);
                    final int uniqueNamesCount = in.readInt();
                    final Set<String> uniqueNames = new HashSet<String>();

                    for (int i = 0; i < uniqueNamesCount; i++) {
                        final int length = in.readShort();

                        final byte[] nameBytes = new byte[length];
                        in.read(nameBytes);
                        uniqueNames.add(new String(nameBytes, "US-ASCII"));
                    }
                    final int cEndRecord = in.readInt();

                    final TransactionLogRecord tlog =
                            new TransactionLogRecord(status, recordLen, headerLen, time, seqNo, crc32, gtrid,
                                uniqueNames, cEndRecord);
                    if (!skipCrcCheck && !tlog.isCrc32Correct()) {
                        throw new CorruptedTransactionLogException("corrupted log found at entry " + entry.getEntryId()
                                + "(invalid CRC, recorded: " + tlog.getCrc32() + ", calculated: "
                                + tlog.calculateCrc32() + ")");
                    }
                    // set entry id for checkpoint
                    tlog.setEntryId(entry.getEntryId());
                    return tlog;
                }
                else {
                    return null;
                }

            }
        }
        // quick path when move out of last entry
        if (this.startEntry > this.lastEntry) {
            return null;
        }
        this.endEntry = this.startEntry + this.batchSize;
        if (this.endEntry > this.lastEntry) {
            this.endEntry = this.lastEntry;
        }
        this.iterator = this.handle.readEntries(this.startEntry, this.endEntry);
        this.startEntry = this.endEntry + 1;
        return this.readLog(skipCrcCheck);
    }
}
