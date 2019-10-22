package org.apache.dolphinscheduler.client.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Tboy
 */
public class ZookeeperOperator {

    private final Logger logger = LoggerFactory.getLogger(ZookeeperOperator.class);

    protected final CuratorFramework client;

    public ZookeeperOperator(CuratorFramework client){
        this.client = client;
    }

    public String get(final String key) {
        try {
            return new String(client.getData().forPath(key), Charset.forName("UTF-8"));
        } catch (Exception ex) {
            logger.error("get key : {}", key, ex);
        }
        return null;
    }

    public List<String> getChildrenKeys(final String key) {
        List<String> values = new ArrayList<String>();
        try {
            values = client.getChildren().forPath(key);
        } catch (Exception ex) {
            logger.error("getChildrenKeys key : {}", key, ex);
        }
        return values;
    }

    public boolean isExisted(final String key) {
        try {
            return client.checkExists().forPath(key) != null;
        } catch (Exception ex) {
            logger.error("isExisted key : {}", key, ex);
        }
        return false;
    }

    public void persist(final String key, final String value) {
        try {
            if (!isExisted(key)) {
                client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath
                        (key, value.getBytes(Charset.forName("utf-8")));
            } else {
                update(key, value);
            }
        } catch (Exception ex) {
            logger.error("persist key : {} , value : {}", key, value, ex);
        }
    }

    public void update(final String key, final String value) {
        try {
            client.inTransaction().check().forPath(key).and().setData().forPath(key, value
                    .getBytes(Charset.forName("UTF-8"))).and().commit();
        } catch (Exception ex) {
            logger.error("update key : {} , value : {}", key, value, ex);
        }
    }

    public void persistEphemeral(final String key, final String value) {
        try {
            if (isExisted(key)) {
                try {
                    client.delete().deletingChildrenIfNeeded().forPath(key);
                } catch (KeeperException.NoNodeException e) {
                    logger.info("{} was already deleted", key);
                }
            }
            client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(key, value.getBytes(Charset.forName("UTF-8")));
        } catch (final Exception ex) {
            logger.error("persistEphemeral key : {} , value : {}", key, value, ex);
        }
    }

    public void persistEphemeral(String key, String value, boolean overwrite) {
        try {
            if (overwrite) {
                persistEphemeral(key, value);
            } else {
                if (!isExisted(key)) {
                    client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(key, value.getBytes(Charset.forName("UTF-8")));
                }
            }
        } catch (final Exception ex) {
            logger.error("persistEphemeral key : {} , value : {}, overwrite : {}", key, value, overwrite, ex);
        }
    }

    public void persistEphemeralSequential(final String key) {
        try {
            client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(key);
        } catch (final Exception ex) {
            logger.error("persistEphemeralSequential key : {}", key, ex);
        }
    }

    public void remove(final String key) {
        try {
            client.delete().deletingChildrenIfNeeded().forPath(key);
        } catch (final Exception ex) {
            logger.error("remove key : {}", key, ex);
        }
    }

    public CuratorFramework getClient() {
        return client;
    }
}
