package org.apache.dolphinscheduler.client.registry;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.dolphinscheduler.client.zookeeper.DefaultEnsembleProvider;
import org.apache.dolphinscheduler.client.zookeeper.ZookeeperCachedOperator;
import org.apache.dolphinscheduler.client.zookeeper.ZookeeperConfig;
import org.apache.dolphinscheduler.remote.utils.StringUtils;
import org.apache.dolphinscheduler.remote.utils.ThreadUtils;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;

import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @Author: Tboy
 */
public class ZookeeperRegistryCenter implements InitializingBean {

    private final Logger logger = LoggerFactory.getLogger(ZookeeperRegistryCenter.class);

    private static final String NAMESPACE = "/dolphinscheduler";

    private static final String NODES = NAMESPACE + "/nodes";

    private static final String TASK_PATH = NAMESPACE + "/nodes/task";

    private static final String APP_PATH = NAMESPACE + "/nodes/app";

    private static final String EMPTY = "";

    @Autowired
    private ZookeeperConfig zookeeperConfig;

    private CuratorFramework client;

    private ZookeeperCachedOperator zookeeperOperator;

    @Override
    public void afterPropertiesSet() throws Exception {
        init();
    }

    public void init(){
        client = buildClient();
        zookeeperOperator = new ZookeeperCachedOperator(client, NODES);
        initNodes();
    }

    public CuratorFramework buildClient() {
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

    private void initNodes() {
        zookeeperOperator.persist(APP_PATH, EMPTY);
        zookeeperOperator.persist(TASK_PATH, EMPTY);
    }

    public void close() {
        try {
            if (null != zookeeperOperator) {
                zookeeperOperator.close();
            }
            ThreadUtils.sleep(500);
            CloseableUtils.closeQuietly(client);
        } catch (Throwable ex) {
            logger.error("ZookeeperRegistryCenter close error", ex);
        }
    }

    public String getTaskPath() {
        return TASK_PATH;
    }

    public String getAppPath() {
        return APP_PATH;
    }

    public Set<String> getTaskNodes() {
        List<String> tasks = zookeeperOperator.getChildrenKeys(TASK_PATH);
        return new HashSet(tasks);
    }

    public void persistTask(String key, String value) throws Exception{
        String taskNamePath = getTaskNamePath(key);
        zookeeperOperator.persist(taskNamePath, value);
    }

    public void persistEphemeralWorker(String taskName, String worker) throws Exception{
        String workerPath = getTaskNamePath(taskName) + "/" + worker;
        zookeeperOperator.persistEphemeral(workerPath, "");
    }

    private String getTaskNamePath(String taskName){
        return this.getTaskPath() + "/" + taskName;
    }

    public ZookeeperConfig getZookeeperConfigConfig() {
        return zookeeperConfig;
    }

    protected String cachePath() {
        return NODES;
    }

    public TreeCache getCache(){
        return zookeeperOperator.getTreeCache();
    }

    public ZookeeperCachedOperator getZookeeperOperator() {
        return zookeeperOperator;
    }
}
