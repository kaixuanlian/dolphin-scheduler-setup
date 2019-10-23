package org.apache.dolphinscheduler.manager;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.dolphinscheduler.client.registry.ZookeeperRegistryCenter;
import org.apache.dolphinscheduler.client.zookeeper.AbstractListener;
import org.apache.dolphinscheduler.client.zookeeper.ZookeeperCachedOperator;
import org.apache.dolphinscheduler.remote.NettyRemotingClient;
import org.apache.dolphinscheduler.remote.command.ExecuteTaskCommand;
import org.apache.dolphinscheduler.remote.config.Address;
import org.apache.dolphinscheduler.remote.exceptions.RemotingException;
import org.apache.dolphinscheduler.remote.utils.Constants;
import org.apache.dolphinscheduler.remote.utils.FastJsonSerializer;
import org.apache.dolphinscheduler.remote.utils.StringUtils;
import org.apache.dolphinscheduler.remote.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @Author: Tboy
 */
@Component
public class ManagerServer implements InitializingBean {

    private final Logger logger = LoggerFactory.getLogger(ManagerServer.class);

    private final ConcurrentHashMap<String, ExecuteTaskCommand> tasks = new ConcurrentHashMap();

    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(ThreadUtils.newThreadFactory("task-thread"));

    @Autowired
    private NettyRemotingClient nettyRemotingClient;

    @Autowired
    private ZookeeperRegistryCenter zookeeperRegistryCenter;

    @Override
    public void afterPropertiesSet() throws Exception {
        this.registerListener();

        this.executorService.scheduleAtFixedRate(new ExecuteTask(), 10, 10, TimeUnit.SECONDS);
    }

    private void registerListener(){
        this.zookeeperRegistryCenter.getZookeeperOperator().addListener(new TaskNodeListener());
    }

    class ExecuteTask implements Runnable{

        @Override
        public void run() {
            if(!tasks.isEmpty()){
                for(Map.Entry<String, ExecuteTaskCommand> entry : tasks.entrySet()){
                    String taskName = entry.getKey();
                    ExecuteTaskCommand command = entry.getValue();
                    Set<String> workerNodes = zookeeperRegistryCenter.getWorkerNodes(taskName);
                    for(String ip : workerNodes){
                        Address address = new Address(ip, command.getConnectorPort());
                        try {
                            nettyRemotingClient.send(address, command.convert2Command());
                        } catch (RemotingException e) {
                            logger.error("execute task : {} error : {}", command, e);
                        }
                    }
                }
            }
        }
    }

    class TaskNodeListener extends AbstractListener {

        @Override
        protected void dataChanged(CuratorFramework client, TreeCacheEvent event, String path) {
            if (zookeeperRegistryCenter.isTaskPath(path)) {
                try {
                    logger.info("TaskNodeListener task path {} , event : {}", path, event.getType());
                    if (event.getType() == TreeCacheEvent.Type.NODE_ADDED || event.getType() == TreeCacheEvent.Type.NODE_UPDATED) {
                        String taskNode = zookeeperRegistryCenter.getZookeeperOperator().get(path);
                        if (StringUtils.isNotEmpty(taskNode)) {
                            logger.info("new task add : {}", taskNode);
                        }
                        tasks.putIfAbsent(path.substring(path.lastIndexOf("/") + 1), FastJsonSerializer.deserialize(taskNode.getBytes(Constants.UTF8), ExecuteTaskCommand.class));
                    }
                } catch (Exception ex) {
                    logger.error("TaskNodeListener capture data change and get data " + "failed.", ex);
                } finally {

                }
            }
        }
    }


}
