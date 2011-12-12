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
        assertTrue(this.ez.readHandles().isEmpty());
        Set<HandleState> sets = new HashSet<HandleState>();
        HandleState state = new HandleState(1, 0);
        sets.add(state);
        this.ez.writeHandles(sets);
        Set<HandleState> idsFromZk = this.ez.readHandles();
        assertFalse(idsFromZk.isEmpty());
        assertEquals(1, idsFromZk.size());
        assertTrue(idsFromZk.contains(state));
    }


    @Test
    public void WriteTwice() throws Exception {
        assertTrue(this.ez.readHandles().isEmpty());
        Set<HandleState> sets = new HashSet<HandleState>();
        HandleState state = new HandleState(1, 0);
        sets.add(state);
        ;
        this.ez.writeHandles(sets);
        Set<HandleState> idsFromZk = this.ez.readHandles();
        assertFalse(idsFromZk.isEmpty());
        assertEquals(1, idsFromZk.size());
        assertTrue(idsFromZk.contains(state));

        sets = new HashSet<HandleState>();
        HandleState state1 = new HandleState(4, 0);
        HandleState state2 = new HandleState(3, 0);
        sets.add(state1);
        sets.add(state2);
        this.ez.writeHandles(sets);

        idsFromZk = this.ez.readHandles();
        assertFalse(idsFromZk.isEmpty());
        assertEquals(2, idsFromZk.size());
        assertTrue(idsFromZk.contains(state1));
        assertTrue(idsFromZk.contains(state2));
        assertFalse(idsFromZk.contains(state));

    }
}
