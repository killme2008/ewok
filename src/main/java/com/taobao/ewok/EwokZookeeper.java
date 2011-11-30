package com.taobao.ewok;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;


/**
 * 
 * @author boyan(boyan@taobao.com)
 * 
 */
public class EwokZookeeper {
    private ZooKeeper zk;
    private EwokConfiguration conf;
    static final Log log = LogFactory.getLog(EwokZookeeper.class);
    private String rootPath;
    private String serverIdPath;


    public EwokZookeeper(EwokConfiguration conf) throws IOException, InterruptedException, KeeperException {
        this.conf = conf;
        zk = new ZooKeeper(conf.getZkServers(), conf.getZkSessionTimeout(), null);
        initRootPath();
        initServerIdPath();
        claimServerIdPath();
    }


    public long readLogId() throws InterruptedException, KeeperException {
        byte[] data = zk.getData(serverIdPath, false, null);
        if (data == null)
            return -1;
        else
            return Long.valueOf(new String(data));
    }


    public void writeLogId(long id) throws InterruptedException, KeeperException {
        zk.setData(serverIdPath, String.valueOf(id).getBytes(), -1);
    }


    private void claimServerIdPath() throws InterruptedException, KeeperException {
        String ownershipPath = serverIdPath + "/ownership";
        zk.create(ownershipPath, conf.getServerId().getBytes(), null, CreateMode.EPHEMERAL);
    }


    private void initServerIdPath() throws InterruptedException, KeeperException {
        this.serverIdPath = rootPath + "/" + conf.getServerId();
        Stat stat = zk.exists(serverIdPath, false);
        if (stat == null) {
            createPersistentPath(serverIdPath);
        }
    }


    private void createPersistentPath(String path) throws InterruptedException, KeeperException {
        try {
            zk.create(path, null, null, CreateMode.PERSISTENT);
        }
        catch (KeeperException e) {
            if (e.code() == Code.NODEEXISTS)
                return;
            else
                throw e;
        }
    }


    private void initRootPath() throws InterruptedException, KeeperException {
        rootPath = "/" + conf.getZkRoot() + "/tms";
        try {
            Stat stat = zk.exists(rootPath, false);
            if (stat == null) {
                createPersistentPath(rootPath);
            }
        }
        catch (KeeperException e) {
            switch (e.code()) {
            case NONODE:
                // Create parent path
                createPersistentPath("/" + conf.getZkRoot());
                // re-init
                initRootPath();
                break;
            default:
                throw e;
            }

        }
    }


    public void close() throws InterruptedException {
        if (this.zk != null)
            this.zk.close();
    }


    ZooKeeper getZkHandle() {
        return this.zk;
    }
}
