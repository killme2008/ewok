package com.taobao.ewok;

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
import static org.junit.Assert.*;


public class BookkeeperJournalUnitTest {
    private BookkeeperJournal journal;


    @Before
    public void setUp() throws Exception {
        this.journal = new BookkeeperJournal();
        Set<Long> ids = this.journal.getEwokZookeeper().readLogIds();
        for (Long id : ids) {
            this.journal.deleteLedger(id);
        }
        this.journal.getEwokZookeeper().writeLogIds(new HashSet<Long>());
    }


    @Test
    public void testOpen() throws Exception {
        this.journal.open();
        assertNotNull(this.journal.getCurrentAppender());
        Set<Long> handles = this.journal.getHandles();
        assertFalse(handles.isEmpty());
        System.out.println(handles);

    }


    @Test
    public void logsCloseOpen() throws Exception {
        this.journal.open();
        Uid commitedUid = UidGenerator.generateUid();
        Set<String> uniqueNames = new HashSet<String>();
        uniqueNames.add("jdbc");
        uniqueNames.add("jms");
        this.journal.log(Status.STATUS_ACTIVE, commitedUid, uniqueNames);
        this.journal.log(Status.STATUS_PREPARING, commitedUid, uniqueNames);
        this.journal.log(Status.STATUS_PREPARED, commitedUid, uniqueNames);
        this.journal.log(Status.STATUS_COMMITTING, commitedUid, uniqueNames);
        this.journal.force();
        this.journal.log(Status.STATUS_COMMITTED, commitedUid, uniqueNames);
        Uid committingUid = UidGenerator.generateUid();
        this.journal.log(Status.STATUS_ACTIVE, committingUid, uniqueNames);
        this.journal.log(Status.STATUS_PREPARING, committingUid, uniqueNames);
        this.journal.log(Status.STATUS_PREPARED, committingUid, uniqueNames);
        this.journal.log(Status.STATUS_COMMITTING, committingUid, uniqueNames);
        this.journal.force();
        checkDanglingRecords(uniqueNames, committingUid);

        this.journal.close();
        this.journal = new BookkeeperJournal();
        this.journal.open();

        checkDanglingRecords(uniqueNames, committingUid);

    }


    private void checkDanglingRecords(Set<String> uniqueNames, Uid committingUid) throws IOException {
        Map<Uid, bitronix.tm.journal.TransactionLogRecord> map = this.journal.collectDanglingRecords();
        assertNotNull(map);
        assertEquals(1, map.size());
        assertTrue(map.containsKey(committingUid));
        bitronix.tm.journal.TransactionLogRecord rc = map.get(committingUid);
        assertEquals(committingUid, rc.getGtrid());
        assertEquals(Status.STATUS_COMMITTING, rc.getStatus());
        assertEquals(uniqueNames, rc.getUniqueNames());
    }


    @After
    public void tearDown() throws Exception {
        this.journal.close();
    }

}
