package com.taobao.ewok;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;


public class EwokConfigurationUnitTest {
    private EwokConfiguration conf;


    @Before
    public void setUp() {
        System.setProperty("ewok.configuration", "ewok-test-config.properties");
        conf = new EwokConfiguration();
    }


    /**
     * <ul>
     * <li>ewok.serverId=test</li>
     * <li>ewok.zkRoot=zkRoot</li>
     * <li>ewok.zkServers=localhost:2182</li>
     * <li>ewok.zkSessionTimeout=10</li>
     * <li>ewok.loadZkPath=/ewok/app2/192-168-1-100</li>
     * <li>ewok.ensembleSize=10</li>
     * <li>ewok.quorumSize=3</li>
     * <li>ewok.password=ewokpass</li>
     * <li>ewok.cursorBatchSize=10</li>
     * <ul>
     */
    @Test
    public void testConf() {
        assertEquals("test", conf.getEwokServerId());
        assertEquals("zkRoot", conf.getZkRoot());
        assertEquals("localhost:2182", conf.getZkServers());
        assertEquals(10, conf.getZkSessionTimeout());
        assertEquals("/ewok/app2/192-168-1-100", conf.getLoadZkPath());
        assertEquals(10, conf.getESize());
        assertEquals(3, conf.getQSize());
        assertEquals("ewokpass", conf.getPassword());
    }


    @After
    public void tearDown() {
        System.clearProperty("ewok.configuration");
    }
}
