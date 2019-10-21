package org.apache.dolphinscheduler.remote;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import org.apache.dolphinscheduler.remote.config.Address;
import org.apache.dolphinscheduler.remote.config.NettyClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author: Tboy
 */
public class NettyRemotingClient {

    private final Logger logger = LoggerFactory.getLogger(NettyRemotingClient.class);

    private final Bootstrap bootstrap = new Bootstrap();

    private final ConcurrentHashMap<Address, Channel> channels = new ConcurrentHashMap();

    private final NettyClientConfig clientConfig;

    public NettyRemotingClient(final NettyClientConfig clientConfig){
        this.clientConfig = clientConfig;
    }


}
