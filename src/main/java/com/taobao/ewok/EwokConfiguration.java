package com.taobao.ewok;

/**
 * A ewok configuration
 * 
 * @author boyan
 * 
 */
public class EwokConfiguration {
    private String zkServers;
    private int zkSessionTimeout = 5000;
    private String zkRoot = "ewok";
    private String initZkPath;
    private String serverId;
    private int eSize = 3;
    private int qSize = 2;
    private String password = "ewok";


    public int getESize() {
        return eSize;
    }


    public void setESize(int eSize) {
        this.eSize = eSize;
    }


    public int getQSize() {
        return qSize;
    }


    public void setQSize(int qSize) {
        this.qSize = qSize;
    }


    public String getPassword() {
        return password;
    }


    public void setPassword(String password) {
        this.password = password;
    }


    public String getServerId() {
        return serverId;
    }


    public void setServerId(String serverId) {
        this.serverId = serverId;
    }


    public String getZkServers() {
        return zkServers;
    }


    public void setZkServers(String zkServers) {
        this.zkServers = zkServers;
    }


    public int getZkSessionTimeout() {
        return zkSessionTimeout;
    }


    public void setZkSessionTimeout(int zkSessionTimeout) {
        this.zkSessionTimeout = zkSessionTimeout;
    }


    public String getInitZkPath() {
        return initZkPath;
    }


    public void setInitZkPath(String initZkPath) {
        this.initZkPath = initZkPath;
    }


    public String getZkRoot() {
        return zkRoot;
    }


    public void setZkRoot(String zkRoot) {
        this.zkRoot = zkRoot;
    }

}
