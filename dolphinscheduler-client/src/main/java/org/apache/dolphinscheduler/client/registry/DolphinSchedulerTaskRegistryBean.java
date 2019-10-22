package org.apache.dolphinscheduler.client.registry;

import org.apache.dolphinscheduler.remote.utils.FastJsonSerializer;

/**
 * @Author: Tboy
 */
public class DolphinSchedulerTaskRegistryBean {

    private String applicationName;

    private String groupName;

    private String taskName;

    private int connectorPort;

    private String description;

    private String className;

    private String methodName;

    public int getConnectorPort() {
        return connectorPort;
    }

    public void setConnectorPort(int connectorPort) {
        this.connectorPort = connectorPort;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String toJson(){
        return FastJsonSerializer.serializeToString(this);
    }
}
