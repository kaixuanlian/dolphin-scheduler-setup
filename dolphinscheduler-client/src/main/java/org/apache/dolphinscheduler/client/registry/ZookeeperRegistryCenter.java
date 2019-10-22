package org.apache.dolphinscheduler.client.registry;

import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.dolphinscheduler.client.zookeeper.ZookeeperCachedOperator;
import org.apache.dolphinscheduler.remote.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @Author: Tboy
 */
@Component
public class ZookeeperRegistryCenter implements InitializingBean {

    private final Logger logger = LoggerFactory.getLogger(ZookeeperRegistryCenter.class);

    private static final String NAMESPACE = "/dolphinscheduler";

    private static final String NODES = NAMESPACE + "/nodes";

    private static final String TASK_PATH = NAMESPACE + "/nodes/task";

    private static final String APP_PATH = NAMESPACE + "/nodes/app";

    private static final String EMPTY = "";

    @Autowired
    private ZookeeperCachedOperator zookeeperOperator;

    @Override
    public void afterPropertiesSet() throws Exception {
        init();
    }

    public void init(){
        zookeeperOperator.start(NODES);
        initNodes();
    }

    private void initNodes() {
        zookeeperOperator.persist(TASK_PATH, EMPTY);
        zookeeperOperator.persist(APP_PATH, EMPTY);
    }

    public void close() {
        try {
            if (null != zookeeperOperator) {
                zookeeperOperator.close();
            }
            ThreadUtils.sleep(500);
        } catch (Throwable ex) {
            logger.error("ZookeeperRegistryCenter close error", ex);
        }
    }

    public boolean isTaskPath(String path) {
        return path != null && path.contains(TASK_PATH) && path.split("/").length == 5;
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
