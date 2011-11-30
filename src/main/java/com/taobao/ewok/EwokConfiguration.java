package com.taobao.ewok;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import bitronix.tm.Configuration;
import bitronix.tm.TransactionManagerServices;
import bitronix.tm.utils.ClassLoaderUtils;
import bitronix.tm.utils.InitializationException;


/**
 * A ewok configuration
 * 
 * @author boyan
 * 
 */
public class EwokConfiguration {
    private String zkServers;
    private int zkSessionTimeout = 10000;
    private String zkRoot = "ewok";
    // Path to load ledger handles,default is null
    private String loadZkPath;
    private String ewokServerId;
    private int eSize = 3;
    private int qSize = 2;
    private String password = "ewok";
    private int cursorBatchSize = 5;
    private Configuration btmConf;

    static Log log = LogFactory.getLog(EwokConfiguration.class);


    static String getString(Properties properties, String key, String defaultValue) {
        String value = System.getProperty(key);
        if (StringUtils.isBlank(value)) {
            value = properties.getProperty(key);
            if (StringUtils.isBlank(value))
                return defaultValue;
        }
        return value;
    }


    static boolean getBoolean(Properties properties, String key, boolean defaultValue) {
        return Boolean.valueOf(getString(properties, key, "" + defaultValue));
    }


    static int getInt(Properties properties, String key, int defaultValue) {
        return Integer.parseInt(getString(properties, key, "" + defaultValue));
    }


    public EwokConfiguration() {
        this.btmConf = TransactionManagerServices.getConfiguration();
        try {
            InputStream in = null;
            Properties properties;
            try {
                String configurationFilename = System.getProperty("ewok.configuration");
                if (configurationFilename != null) {
                    if (log.isDebugEnabled())
                        log.debug("loading configuration file " + configurationFilename);
                    try {
                        in = new FileInputStream(configurationFilename);
                    }
                    catch (FileNotFoundException e) {
                        log.warn("Could not find absolute path: " + configurationFilename
                                + ",try to load it in classpath");
                        in = ClassLoaderUtils.getResourceAsStream(configurationFilename);
                    }
                }
                else {
                    if (log.isDebugEnabled())
                        log.debug("loading default configuration");
                    in = ClassLoaderUtils.getResourceAsStream("ewok-config.properties");
                }
                properties = new Properties();
                if (in != null)
                    properties.load(in);
                else if (log.isDebugEnabled())
                    log.debug("no configuration file found, using default settings");
            }
            finally {
                if (in != null)
                    in.close();
            }
            this.zkRoot = getString(properties, "ewok.zkRoot", "ewok");
            this.zkServers = getString(properties, "ewok.zkServers", "localhost:2181");
            this.zkSessionTimeout = getInt(properties, "ewok.zkSessionTimeout", 5000);
            this.loadZkPath = getString(properties, "ewok.loadZkPath", null);
            this.ewokServerId = getString(properties, "ewok.serverId", getLocalHostAddress().toString());
            this.eSize = getInt(properties, "ewok.ensembleSize", 3);
            this.qSize = getInt(properties, "ewok.quorumSize", 2);
            this.password = getString(properties, "ewok.password", "ewok");
            this.cursorBatchSize = getInt(properties, "ewok.cursorBatchSize", 5);
        }
        catch (IOException ex) {
            throw new InitializationException("error loading configuration", ex);
        }
    }


    // Try to find a valid ipv4 address for using
    public static InetAddress getLocalHostAddress() throws UnknownHostException, SocketException {
        final Enumeration<NetworkInterface> enumeration = NetworkInterface.getNetworkInterfaces();
        InetAddress ipv6Address = null;
        while (enumeration.hasMoreElements()) {
            final NetworkInterface networkInterface = enumeration.nextElement();
            final Enumeration<InetAddress> en = networkInterface.getInetAddresses();
            while (en.hasMoreElements()) {
                final InetAddress address = en.nextElement();
                if (!address.isLoopbackAddress()) {
                    if (address instanceof Inet6Address) {
                        ipv6Address = address;
                    }
                    else {
                        // 优先使用ipv4
                        return address;
                    }
                }
            }

        }
        // 没有ipv4，则使用ipv6
        if (ipv6Address != null) {
            return ipv6Address;
        }
        return InetAddress.getLocalHost();
    }


    public Configuration getBtmConf() {
        return btmConf;
    }


    public void setBtmConf(Configuration btmConf) {
        this.btmConf = btmConf;
    }


    public int getCursorBatchSize() {
        return cursorBatchSize;
    }


    public void setCursorBatchSize(int cursorBatchSize) {
        this.cursorBatchSize = cursorBatchSize;
    }


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


    public String getLoadZkPath() {
        return loadZkPath;
    }


    public void setLoadZkPath(String loadZkPath) {
        this.loadZkPath = loadZkPath;
    }


    public String getEwokServerId() {
        return ewokServerId;
    }


    public void setEwokServerId(String ewokServerId) {
        this.ewokServerId = ewokServerId;
    }


    public String getZkRoot() {
        return zkRoot;
    }


    public void setZkRoot(String zkRoot) {
        this.zkRoot = zkRoot;
    }

}
