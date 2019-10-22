package org.apache.dolphinscheduler.remote.config;

import org.apache.dolphinscheduler.remote.utils.Constants;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * @Author: Tboy
 */
@Component
public class NettyServerConfig {

    @Value("${netty.sobacklog:1024}")
    private int soBacklog;

    @Value("${netty.tcpNodelay:true}")
    private boolean tcpNodelay;

    @Value("${netty.soKeepalive:true}")
    private boolean soKeepalive;

    @Value("${netty.sendBufferSize:65535}")
    private int sendBufferSize;

    @Value("${netty.receiveBufferSize:65535}")
    private int receiveBufferSize;

    @Value("${netty.workerThread:16}")
    private int workerThread;

    @Value("${netty.listenPort:12357}")
    private int listenPort;

    public int getListenPort() {
        return listenPort;
    }

    public void setListenPort(int listenPort) {
        this.listenPort = listenPort;
    }

    public int getSoBacklog() {
        return soBacklog;
    }

    public void setSoBacklog(int soBacklog) {
        this.soBacklog = soBacklog;
    }

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

    public int getWorkerThread() {
        return workerThread;
    }

    public void setWorkerThread(int workerThread) {
        this.workerThread = workerThread;
    }
}
