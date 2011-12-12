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
    private long lastEntry;
    private LedgerHandle handle;
    private long startEntry;
    private long endEntry;
    private int batchSize = 5;
    private Enumeration<LedgerEntry> iterator;


    public LedgerCursor(long lastEntry, int batchSize, LedgerHandle handle) {
        super();
        this.lastEntry = lastEntry;
        this.batchSize = batchSize;
        this.handle = handle;
    }


    public LedgerHandle getHandle() {
        return handle;
    }


    public void close() throws BKException, InterruptedException {
        this.handle.close();
    }


    public TransactionLogRecord readLog() throws IOException, BKException, InterruptedException {
        return readLog(false);
    }


    public synchronized TransactionLogRecord readLog(boolean skipCrcCheck) throws IOException, BKException,
            InterruptedException {
        while (iterator != null && iterator.hasMoreElements()) {
            LedgerEntry entry = iterator.nextElement();
            if (entry != null) {
                DataInputStream in = new DataInputStream(entry.getEntryInputStream());
                if (in.available() > 4) {
                    int status = in.readInt();
                    int recordLen = in.readInt();
                    int headerLen = in.readInt();
                    long time = in.readLong();
                    int seqNo = in.readInt();
                    int crc32 = in.readInt();
                    byte gtridLen = in.readByte();
                    byte[] gtridArray = new byte[gtridLen];
                    in.read(gtridArray);
                    Uid gtrid = new Uid(gtridArray);
                    int uniqueNamesCount = in.readInt();
                    Set<String> uniqueNames = new HashSet<String>();

                    for (int i = 0; i < uniqueNamesCount; i++) {
                        int length = in.readShort();

                        byte[] nameBytes = new byte[length];
                        in.read(nameBytes);
                        uniqueNames.add(new String(nameBytes, "US-ASCII"));
                    }
                    int cEndRecord = in.readInt();

                    TransactionLogRecord tlog =
                            new TransactionLogRecord(status, recordLen, headerLen, time, seqNo, crc32, gtrid,
                                uniqueNames, cEndRecord);
                    if (!skipCrcCheck && !tlog.isCrc32Correct()) {
                        throw new CorruptedTransactionLogException("corrupted log found at entry " + entry.getEntryId()
                                + "(invalid CRC, recorded: " + tlog.getCrc32() + ", calculated: "
                                + tlog.calculateCrc32() + ")");
                    }
                    return tlog;
                }
                else
                    return null;

            }
        }
        // quick path when move out of last entry
        if (startEntry > lastEntry)
            return null;
        endEntry = startEntry + batchSize;
        if (endEntry > lastEntry) {
            endEntry = lastEntry;
        }
        iterator = handle.readEntries(startEntry, endEntry);
        startEntry = endEntry + 1;
        return readLog(skipCrcCheck);
    }
}
