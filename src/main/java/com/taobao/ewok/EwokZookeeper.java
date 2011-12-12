package com.taobao.ewok;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;


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


    public Set<HandleState> readHandles() throws InterruptedException, KeeperException {
        return this.readHandles(this.serverIdPath);
    }


    public Set<HandleState> readHandles(String path) throws InterruptedException, KeeperException {
        if (StringUtils.isBlank(path))
            return Collections.emptySet();
        byte[] data = zk.getData(path, false, null);
        if (data == null)
            return Collections.emptySet();
        else {
            String json = new String(data);
            if (StringUtils.isEmpty(json.trim()))
                return Collections.emptySet();
            Set<Map> set = JSON.parseObject(json, Set.class);
            Set<HandleState> rt = new HashSet<HandleState>();
            for (Map map : set) {
                rt.add(new HandleState(getLong(map.get("id")), getLong(map.get("checkpoint"))));
            }
            return rt;
        }
    }


    private long getLong(Object v) {
        return Long.valueOf(String.valueOf(v));
    }

    static final String SPLIT = ",";


    public void writeHandles(Set<HandleState> ids) throws InterruptedException, KeeperException {
        zk.setData(serverIdPath, JSON.toJSONString(ids).getBytes(), -1);
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
