package com.taobao.ewok;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;


/**
 * Ewok use this class to talk with zookeeper,the tree in zookeeper:</br> /ewok/<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;/btmServerId1</br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;/ewokServerId1</br>
 * &nbsp;&nbsp;
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;/ownership</br>
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
        zk = new ZooKeeper(conf.getZkServers(), conf.getZkSessionTimeout(), new Watcher() {

            public void process(WatchedEvent event) {
                if (log.isDebugEnabled())
                    log.debug("Processing event " + event);

            }
        });
        initRootPath();
        initServerIdPath();
        claimServerIdPath();
    }


    String getRootPath() {
        return rootPath;
    }


    String getServerIdPath() {
        return serverIdPath;
    }


    public Set<Long> readLogIds() throws InterruptedException, KeeperException {
        return this.readLogIds(this.serverIdPath);
    }


    public Set<Long> readLogIds(String path) throws InterruptedException, KeeperException {
        if (StringUtils.isBlank(path))
            return Collections.emptySet();
        byte[] data = zk.getData(path, false, null);
        if (data == null)
            return Collections.emptySet();
        else {
            String[] tmps = new String(data).split(SPLIT);
            Set<Long> rt = new HashSet<Long>();
            for (String tmp : tmps) {
                if (!StringUtils.isBlank(tmp))
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
        zk.create(ownershipPath, conf.getEwokServerId().getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }


    private void initServerIdPath() throws InterruptedException, KeeperException {
        this.serverIdPath = rootPath + "/" + conf.getEwokServerId();
        Stat stat = zk.exists(serverIdPath, false);
        if (stat == null) {
            createPersistentPath(serverIdPath);
        }
    }


    private void createPersistentPath(String path) throws InterruptedException, KeeperException {
        try {
            zk.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        catch (KeeperException e) {
            if (e.code() == Code.NODEEXISTS)
                return;
            else
                throw e;
        }
    }


    private void initRootPath() throws InterruptedException, KeeperException {
        rootPath = "/" + conf.getZkRoot() + "/" + conf.getBtmConf().getServerId();
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
