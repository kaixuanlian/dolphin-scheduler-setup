package org.apache.dolphinscheduler.client.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.dolphinscheduler.remote.utils.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Tboy
 */
@Component
public class ZookeeperOperator implements InitializingBean {

    private final Logger logger = LoggerFactory.getLogger(ZookeeperOperator.class);

    @Autowired
    private ZookeeperConfig zookeeperConfig;

    protected CuratorFramework client;

    @Override
    public void afterPropertiesSet() throws Exception {
        this.client = buildClient();
    }

    private CuratorFramework buildClient() {
        logger.info("zookeeper registry center init, server lists is: {}.", zookeeperConfig.getServerLists());
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder().ensembleProvider(new DefaultEnsembleProvider(zookeeperConfig.getServerLists())).retryPolicy(new ExponentialBackoffRetry(zookeeperConfig.getBaseSleepTimeMs(), zookeeperConfig.getMaxRetries(), zookeeperConfig.getMaxSleepMs()));
        if (0 != zookeeperConfig.getSessionTimeoutMs()) {
            builder.sessionTimeoutMs(zookeeperConfig.getSessionTimeoutMs());
        }
        if (0 != zookeeperConfig.getConnectionTimeoutMs()) {
            builder.connectionTimeoutMs(zookeeperConfig.getConnectionTimeoutMs());
        }
        if (StringUtils.isNotBlank(zookeeperConfig.getDigest())) {
            builder.authorization("digest", zookeeperConfig.getDigest().getBytes(Charset.forName("UTF-8"))).aclProvider(new ACLProvider() {

                @Override
                public List<ACL> getDefaultAcl() {
                    return ZooDefs.Ids.CREATOR_ALL_ACL;
                }

                @Override
                public List<ACL> getAclForPath(final String path) {
                    return ZooDefs.Ids.CREATOR_ALL_ACL;
                }
            });
        }
        client = builder.build();
        client.start();
        try {
            client.blockUntilConnected();
        } catch (final Exception ex) {
            throw new RuntimeException(ex);
        }
        return client;
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

    public void close(){
        CloseableUtils.closeQuietly(client);
    }

}
