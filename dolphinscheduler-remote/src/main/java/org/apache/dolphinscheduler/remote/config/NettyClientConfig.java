package org.apache.dolphinscheduler.remote.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * @Author: Tboy
 */
@Component
public class NettyClientConfig {

    @Value("${netty.tcpNodelay:true}")
    private boolean tcpNodelay;

    @Value("${netty.soKeepalive:true}")
    private boolean soKeepalive;

    @Value("${netty.sendBufferSize:65535}")
    private int sendBufferSize;

    @Value("${netty.receiveBufferSize:65535}")
    private int receiveBufferSize;

    @Value("${netty.connectorPort:12356}")
    private int connectorPort;

    public boolean isTcpNodelay() {
        return tcpNodelay;
    }

    public void setTcpNodelay(boolean tcpNodelay) {
        this.tcpNodelay = tcpNodelay;
    }

    public boolean isSoKeepalive() {
        return soKeepalive;
    }

    public void setSoKeepalive(boolean soKeepalive) {
        this.soKeepalive = soKeepalive;
    }

    public int getSendBufferSize() {
        return sendBufferSize;
    }

    public void setSendBufferSize(int sendBufferSize) {
        this.sendBufferSize = sendBufferSize;
    }

    public int getReceiveBufferSize() {
        return receiveBufferSize;
    }

    public void setReceiveBufferSize(int receiveBufferSize) {
        this.receiveBufferSize = receiveBufferSize;
    }

    public int getConnectorPort() {
        return connectorPort;
    }

    public void setConnectorPort(int connectorPort) {
        this.connectorPort = connectorPort;
    }
}
