package org.apache.dolphinscheduler.remote.config;

/**
 * @Author: Tboy
 */
public class NettyClientConfig {

    private int sendBufferSize = 65535;

    private int receiveBufferSize = 65535;

    private int port = 12356;

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

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
