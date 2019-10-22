package org.apache.dolphinscheduler.client.zookeeper;


import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * @Author: Tboy
 */
@Component
public class ZookeeperConfig {

    @Value("${zk.conf.serverLists}")
    private String serverLists;

    @Value("${zk.conf.baseSleepTimeMs:100}")
    private int baseSleepTimeMs;

    @Value("${zk.conf.maxSleepMs:30000}")
    private int maxSleepMs;

    @Value("${zk.conf.maxRetries:10}")
    private int maxRetries;

    @Value("${zk.conf.sessionTimeoutMs:60000}")
    private int sessionTimeoutMs;

    @Value("${zk.conf.connectionTimeoutMs:30000}")
    private int connectionTimeoutMs;

    @Value("${zk.conf.digest: }")
    private String digest;

    public ZookeeperConfig(final String serverLists, final int baseSleepTimeMs, final int maxSleepMs, final int maxRetries) {
        this.serverLists = serverLists;
        this.baseSleepTimeMs = baseSleepTimeMs;
        this.maxSleepMs = maxSleepMs;
        this.maxRetries = maxRetries;
    }

    public ZookeeperConfig() {
    }

    public String getServerLists() {
        return serverLists;
    }

    public void setServerLists(String serverLists) {
        this.serverLists = serverLists;
    }

    public int getBaseSleepTimeMs() {
        return baseSleepTimeMs;
    }

    public void setBaseSleepTimeMs(int baseSleepTimeMs) {
        this.baseSleepTimeMs = baseSleepTimeMs;
    }

    public int getMaxSleepMs() {
        return maxSleepMs;
    }

    public void setMaxSleepMs(int maxSleepMs) {
        this.maxSleepMs = maxSleepMs;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }


    public int getSessionTimeoutMs() {
        return sessionTimeoutMs;
    }

    public void setSessionTimeoutMs(int sessionTimeoutMs) {
        this.sessionTimeoutMs = sessionTimeoutMs;
    }

    public int getConnectionTimeoutMs() {
        return connectionTimeoutMs;
    }

    public void setConnectionTimeoutMs(int connectionTimeoutMs) {
        this.connectionTimeoutMs = connectionTimeoutMs;
    }

    public String getDigest() {
        return digest;
    }

    public void setDigest(String digest) {
        this.digest = digest;
    }
}
