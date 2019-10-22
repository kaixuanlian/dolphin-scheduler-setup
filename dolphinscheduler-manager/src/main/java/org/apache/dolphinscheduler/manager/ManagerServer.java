package org.apache.dolphinscheduler.manager;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.dolphinscheduler.client.registry.ZookeeperRegistryCenter;
import org.apache.dolphinscheduler.client.zookeeper.AbstractListener;
import org.apache.dolphinscheduler.client.zookeeper.ZookeeperCachedOperator;
import org.apache.dolphinscheduler.remote.NettyRemotingClient;
import org.apache.dolphinscheduler.remote.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Set;

/**
 * @Author: Tboy
 */
@Component
public class ManagerServer implements InitializingBean {

    private final Logger logger = LoggerFactory.getLogger(ManagerServer.class);

    @Autowired
    private NettyRemotingClient nettyRemotingClient;

    @Autowired
    private ZookeeperRegistryCenter zookeeperRegistryCenter;


    @Override
    public void afterPropertiesSet() throws Exception {
        registerListener();
    }

    private void registerListener(){
        zookeeperRegistryCenter.getZookeeperOperator().addListener(new TaskNodeListener());
    }

    class TaskNodeListener extends AbstractListener {

        @Override
        protected void dataChanged(CuratorFramework client, TreeCacheEvent event, String path) {
            if (zookeeperRegistryCenter.isTaskPath(path)) {
                try {
                    if (event.getType() == TreeCacheEvent.Type.NODE_ADDED) {
                        String taskNode = zookeeperRegistryCenter.getZookeeperOperator().get(path);
                        if (StringUtils.isNotEmpty(taskNode)) {
                            logger.info("new task add : {}", taskNode);
                        }
                    }
                } catch (Exception ex) {
                    logger.error("TaskNodeListener capture data change and get data " + "failed.", ex);
                } finally {

                }
            }
        }
    }


}
