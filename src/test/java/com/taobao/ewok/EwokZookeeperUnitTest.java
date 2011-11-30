package com.taobao.ewok;

import java.util.HashSet;
import java.util.Set;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;


public class EwokZookeeperUnitTest {

    private EwokZookeeper ez;
    private EwokConfiguration conf;


    @Before
    public void setUp() throws Exception {
        conf = new EwokConfiguration();
        ez = new EwokZookeeper(conf);
    }


    @After
    public void tearDown() throws Exception {
        ZooKeeper zk = ez.getZkHandle();
        zk.delete(ez.getServerIdPath() + "/ownership", -1);
        zk.delete(ez.getServerIdPath(), -1);
        ez.close();

    }


    @Test
    public void testInit() throws Exception {
        ZooKeeper zk = ez.getZkHandle();
        String ownerPath = ez.getServerIdPath() + "/ownership";
        Stat stat = zk.exists(ownerPath, false);
        byte[] data = zk.getData(ownerPath, null, stat);
        assertNotNull(stat);
        assertEquals(new String(data), conf.getEwokServerId());
    }


    @Test
    public void testReadIdsWriteIdsReadIds() throws Exception {
        assertTrue(this.ez.readLogIds().isEmpty());
        Set<Long> sets = new HashSet<Long>();
        sets.add(1L);
        this.ez.writeLogIds(sets);
        Set<Long> idsFromZk = this.ez.readLogIds();
        assertFalse(idsFromZk.isEmpty());
        assertEquals(1, idsFromZk.size());
        assertTrue(idsFromZk.contains(1L));
    }


    @Test
    public void WriteTwice() throws Exception {
        assertTrue(this.ez.readLogIds().isEmpty());
        Set<Long> sets = new HashSet<Long>();
        sets.add(1L);
        this.ez.writeLogIds(sets);
        Set<Long> idsFromZk = this.ez.readLogIds();
        assertFalse(idsFromZk.isEmpty());
        assertEquals(1, idsFromZk.size());
        assertTrue(idsFromZk.contains(1L));

        sets = new HashSet<Long>();
        sets.add(4L);
        sets.add(3L);
        this.ez.writeLogIds(sets);

        idsFromZk = this.ez.readLogIds();
        assertFalse(idsFromZk.isEmpty());
        assertEquals(2, idsFromZk.size());
        assertTrue(idsFromZk.contains(3L));
        assertTrue(idsFromZk.contains(4L));
        assertFalse(idsFromZk.contains(1L));

    }
}
