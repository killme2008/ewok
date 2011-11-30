package com.taobao.ewok;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import javax.transaction.Status;

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
    public void testWriteLogGetCursorRead() throws Exception {
        Uid uid = UidGenerator.generateUid();
        Set<String> resourceNames = new HashSet<String>();
        resourceNames.add("jdbc");
        resourceNames.add("jms");

        // Adds some records
        TransactionLogRecord tlog = new TransactionLogRecord(Status.STATUS_ACTIVE, uid, resourceNames);
        this.appender.writeLog(tlog);

        tlog = new TransactionLogRecord(Status.STATUS_PREPARING, uid, resourceNames);
        this.appender.writeLog(tlog);

        tlog = new TransactionLogRecord(Status.STATUS_PREPARED, uid, resourceNames);
        this.appender.writeLog(tlog);

        tlog = new TransactionLogRecord(Status.STATUS_COMMITTING, uid, resourceNames);
        this.appender.writeLog(tlog);

        tlog = new TransactionLogRecord(Status.STATUS_COMMITTED, uid, resourceNames);
        this.appender.writeLog(tlog);
        this.appender.close();

        // Get cursor to read logs
        LedgerCursor cursor = this.appender.getCursor();
        assertNotNull(cursor);
        TransactionLogRecord record = cursor.readLog(false);
        assertRecord(uid, resourceNames, Status.STATUS_ACTIVE, record);
        record = cursor.readLog(false);
        assertRecord(uid, resourceNames, Status.STATUS_PREPARING, record);
        record = cursor.readLog(false);
        assertRecord(uid, resourceNames, Status.STATUS_PREPARED, record);
        record = cursor.readLog(false);
        assertRecord(uid, resourceNames, Status.STATUS_COMMITTING, record);
        record = cursor.readLog(false);
        assertRecord(uid, resourceNames, Status.STATUS_COMMITTED, record);
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
