package com.taobao.ewok;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.transaction.Status;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import bitronix.tm.utils.Uid;
import bitronix.tm.utils.UidGenerator;


public class BookkeeperJournalUnitTest {
    private BookkeeperJournal journal;


    @Before
    public void setUp() throws Exception {
        this.journal = new BookkeeperJournal();
        final Set<HandleState> states = this.journal.getEwokZookeeper().readHandles();
        for (final HandleState state : states) {
            this.journal.deleteLedger(state.id);
        }
        this.journal.getEwokZookeeper().writeHandles(new HashSet<HandleState>());
    }


    @Test
    public void testOpen() throws Exception {
        this.journal.open();
        assertNotNull(this.journal.getCurrentAppender());
        final Set<HandleState> handles = this.journal.getHandles();
        assertFalse(handles.isEmpty());
        System.out.println(handles);

    }


    @Test
    public void swapLogs() throws Exception {
        this.journal.getConf().getBtmConf().setMaxLogSizeInMb(1);
        this.journal.open();

        final LedgerAppender currApd = this.journal.getCurrentAppender();
        for (int i = 0; i < 10000; i++) {
            final Uid commitedUid = UidGenerator.generateUid();
            final Set<String> uniqueNames = new HashSet<String>();
            uniqueNames.add("jdbc");
            uniqueNames.add("jms");
            this.journal.log(Status.STATUS_ACTIVE, commitedUid, uniqueNames);
            this.journal.log(Status.STATUS_PREPARING, commitedUid, uniqueNames);
            this.journal.log(Status.STATUS_PREPARED, commitedUid, uniqueNames);
            this.journal.log(Status.STATUS_COMMITTING, commitedUid, uniqueNames);
            this.journal.force();
            if (i % 2 == 0) {
                this.journal.log(Status.STATUS_COMMITTED, commitedUid, uniqueNames);
            }
        }
        this.journal.force();
        final Map<Uid, bitronix.tm.journal.TransactionLogRecord> map = this.journal.collectDanglingRecords();
        assertNotNull(map);
        assertEquals(5000, map.size());
        assertFalse(currApd == this.journal.getCurrentAppender());
    }


    @Test
    public void logsCloseOpen() throws Exception {
        this.journal.open();
        final Uid commitedUid = UidGenerator.generateUid();
        final Set<String> uniqueNames = new HashSet<String>();
        uniqueNames.add("jdbc");
        uniqueNames.add("jms");
        this.journal.log(Status.STATUS_ACTIVE, commitedUid, uniqueNames);
        this.journal.log(Status.STATUS_PREPARING, commitedUid, uniqueNames);
        this.journal.log(Status.STATUS_PREPARED, commitedUid, uniqueNames);
        this.journal.log(Status.STATUS_COMMITTING, commitedUid, uniqueNames);
        this.journal.force();
        this.journal.log(Status.STATUS_COMMITTED, commitedUid, uniqueNames);
        final Uid committingUid = UidGenerator.generateUid();
        this.journal.log(Status.STATUS_ACTIVE, committingUid, uniqueNames);
        this.journal.log(Status.STATUS_PREPARING, committingUid, uniqueNames);
        this.journal.log(Status.STATUS_PREPARED, committingUid, uniqueNames);
        this.journal.log(Status.STATUS_COMMITTING, committingUid, uniqueNames);
        this.journal.force();
        this.checkDanglingRecords(uniqueNames, committingUid);

        this.journal.close();
        this.journal = new BookkeeperJournal();
        this.journal.open();

        this.checkDanglingRecords(uniqueNames, committingUid);

    }


    private void checkDanglingRecords(final Set<String> uniqueNames, final Uid committingUid) throws IOException {
        final Map<Uid, bitronix.tm.journal.TransactionLogRecord> map = this.journal.collectDanglingRecords();
        assertNotNull(map);
        assertEquals(1, map.size());
        assertTrue(map.containsKey(committingUid));
        final bitronix.tm.journal.TransactionLogRecord rc = map.get(committingUid);
        assertEquals(committingUid, rc.getGtrid());
        assertEquals(Status.STATUS_COMMITTING, rc.getStatus());
        assertEquals(uniqueNames, rc.getUniqueNames());
    }


    @After
    public void tearDown() throws Exception {
        this.journal.close();
    }

}
