package org.apache.dolphinscheduler.remote.command;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.dolphinscheduler.remote.utils.FastJsonSerializer;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Author: Tboy
 */
public class ExecuteTaskCommand implements Serializable {

    private static final AtomicLong REQUEST = new AtomicLong(1);

    private String applicationName;

    private String groupName;

    private String taskName;

    private int connectorPort;

    private String description;

    private String className;

    private String methodName;

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

    public int getConnectorPort() {
        return connectorPort;
    }

    public void setConnectorPort(int connectorPort) {
        this.connectorPort = connectorPort;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
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

    public Command convert2Command(){
        Command command = new Command(REQUEST.getAndIncrement());
        command.setType(CommandType.EXECUTE_TASK);
        byte[] body = FastJsonSerializer.serialize(this);
        ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer(body.length);
        buffer.writeBytes(body);
        command.setBody(buffer);
        return command;
    }
}
