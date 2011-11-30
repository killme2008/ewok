package com.taobao.ewok;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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


    public Set<Long> readLogIds() throws InterruptedException, KeeperException {
        byte[] data = zk.getData(serverIdPath, false, null);
        if (data == null)
            return Collections.emptySet();
        else {
            String[] tmps = new String(data).split(SPLIT);
            Set<Long> rt = new HashSet<Long>();
            for (String tmp : tmps) {
                rt.add(Long.valueOf(tmp));

            }
            return rt;
        }
    }

    static final String SPLIT = ",";


    private String join(Set<Long> ids) {
        if (ids == null)
            return "";
        StringBuilder sb = new StringBuilder();
        boolean wasFirst = true;
        for (long id : ids) {
            if (wasFirst) {
                sb.append(id);
                wasFirst = false;
            }
            else {
                sb.append(",").append(id);
            }
        }
        return sb.toString();
    }


    public void writeLogIds(Set<Long> ids) throws InterruptedException, KeeperException {
        zk.setData(serverIdPath, join(ids).getBytes(), -1);
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
