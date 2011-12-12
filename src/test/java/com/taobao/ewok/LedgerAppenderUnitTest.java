package com.taobao.ewok;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import javax.transaction.Status;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import bitronix.tm.utils.Uid;
import bitronix.tm.utils.UidGenerator;
import static org.junit.Assert.*;


public class LedgerAppenderUnitTest {
    private LedgerAppender appender;
    private BookKeeper bookKeeper;


    @Before
    public void setUp() throws Exception {
        EwokConfiguration conf = new EwokConfiguration();
        conf.setESize(1);
        conf.setQSize(1);
        this.bookKeeper = new BookKeeper(conf.getZkServers());
        this.appender = BookkeeperJournal.createNewLedgerAppender(bookKeeper, conf);
    }


    @Test
    public void testWriteLogsGetCursorReadLogs() throws Exception {
        Uid uid1 = UidGenerator.generateUid();
        Set<String> resourceNames = new HashSet<String>();
        resourceNames.add("jdbc");
        resourceNames.add("jms");

        // Adds some records
        TransactionLogRecord tlog = new TransactionLogRecord(Status.STATUS_ACTIVE, uid1, resourceNames);
        this.appender.writeLog(tlog);

        tlog = new TransactionLogRecord(Status.STATUS_PREPARING, uid1, resourceNames);
        this.appender.writeLog(tlog);

        tlog = new TransactionLogRecord(Status.STATUS_PREPARED, uid1, resourceNames);
        this.appender.writeLog(tlog);

        tlog = new TransactionLogRecord(Status.STATUS_COMMITTING, uid1, resourceNames);
        this.appender.writeLog(tlog);

        tlog = new TransactionLogRecord(Status.STATUS_COMMITTED, uid1, resourceNames);
        this.appender.writeLog(tlog);

        // Get cursor to read logs
        this.appender.force();
        LedgerCursor cursor = this.appender.getCursor();
        assertNotNull(cursor);
        TransactionLogRecord record = cursor.readLog(false);
        assertRecord(uid1, resourceNames, Status.STATUS_ACTIVE, record);
        record = cursor.readLog(false);
        assertRecord(uid1, resourceNames, Status.STATUS_PREPARING, record);
        record = cursor.readLog(false);
        assertRecord(uid1, resourceNames, Status.STATUS_PREPARED, record);
        record = cursor.readLog(false);
        assertRecord(uid1, resourceNames, Status.STATUS_COMMITTING, record);
        record = cursor.readLog(false);
        assertRecord(uid1, resourceNames, Status.STATUS_COMMITTED, record);
        assertNull(cursor.readLog());

        // Write again
        Uid uid2 = UidGenerator.generateUid();
        tlog = new TransactionLogRecord(Status.STATUS_ACTIVE, uid2, resourceNames);
        this.appender.writeLog(tlog);
        tlog = new TransactionLogRecord(Status.STATUS_ROLLEDBACK, uid2, resourceNames);
        this.appender.writeLog(tlog);
        this.appender.force();

        checkLogs(uid1, resourceNames, uid2);

        // close appender
        this.appender.close();

        // Read again
        checkLogs(uid1, resourceNames, uid2);

    }


    private void checkLogs(Uid uid1, Set<String> resourceNames, Uid uid2) throws BKException, InterruptedException,
            IOException {
        LedgerCursor cursor;
        TransactionLogRecord record;
        cursor = this.appender.getCursor();
        assertNotNull(cursor);
        record = cursor.readLog(false);
        assertRecord(uid1, resourceNames, Status.STATUS_ACTIVE, record);
        record = cursor.readLog(false);
        assertRecord(uid1, resourceNames, Status.STATUS_PREPARING, record);
        record = cursor.readLog(false);
        assertRecord(uid1, resourceNames, Status.STATUS_PREPARED, record);
        record = cursor.readLog(false);
        assertRecord(uid1, resourceNames, Status.STATUS_COMMITTING, record);
        record = cursor.readLog(false);
        assertRecord(uid1, resourceNames, Status.STATUS_COMMITTED, record);
        record = cursor.readLog(false);
        assertRecord(uid2, resourceNames, Status.STATUS_ACTIVE, record);
        record = cursor.readLog(false);
        assertRecord(uid2, resourceNames, Status.STATUS_ROLLEDBACK, record);
        assertNull(cursor.readLog());
    }


    private void assertRecord(Uid uid, Set<String> uniqNames, int status, TransactionLogRecord log) {
        assertNotNull(log);
        assertEquals(status, log.getStatus());
        assertEquals(uid, log.getGtrid());
        assertEquals(uniqNames, log.getUniqueNames());
    }


    @After
    public void tearDown() throws Exception {
        long id = this.appender.getHandle().getId();
        this.appender.close();
        this.bookKeeper.deleteLedger(id);
    }
}
