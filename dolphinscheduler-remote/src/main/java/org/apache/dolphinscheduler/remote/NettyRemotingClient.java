package org.apache.dolphinscheduler.remote;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.dolphinscheduler.remote.codec.NettyDecoder;
import org.apache.dolphinscheduler.remote.codec.NettyEncoder;
import org.apache.dolphinscheduler.remote.command.Command;
import org.apache.dolphinscheduler.remote.command.CommandType;
import org.apache.dolphinscheduler.remote.config.Address;
import org.apache.dolphinscheduler.remote.config.NettyClientConfig;
import org.apache.dolphinscheduler.remote.exceptions.RemotingException;
import org.apache.dolphinscheduler.remote.handler.NettyClientHandler;
import org.apache.dolphinscheduler.remote.processor.NettyRequestProcessor;
import org.apache.dolphinscheduler.remote.utils.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author: Tboy
 */
@Component
public class NettyRemotingClient {

    private final Logger logger = LoggerFactory.getLogger(NettyRemotingClient.class);

    private final Bootstrap bootstrap = new Bootstrap();

    private final NettyEncoder encoder = new NettyEncoder();

    private final ConcurrentHashMap<Address, Channel> channels = new ConcurrentHashMap();

    private final ExecutorService defaultExecutor = Executors.newFixedThreadPool(Constants.CPUS);

    private final NioEventLoopGroup workGroup = new NioEventLoopGroup(1, new ThreadFactory() {
        private AtomicInteger threadIndex = new AtomicInteger(0);

        public Thread newThread(Runnable r) {
            return new Thread(r, String.format("NettyClient_%d", this.threadIndex.incrementAndGet()));
        }
    });

    private final NettyClientHandler clientHandler = new NettyClientHandler(this);

    private final NettyClientConfig clientConfig;

    public NettyRemotingClient(final NettyClientConfig clientConfig){
        this.clientConfig = clientConfig;
    }

    @PostConstruct
    public void start(){

        this.bootstrap
                .group(this.workGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, clientConfig.isSoKeepalive())
                .option(ChannelOption.TCP_NODELAY, clientConfig.isTcpNodelay())
                .option(ChannelOption.SO_SNDBUF, clientConfig.getSendBufferSize())
                .option(ChannelOption.SO_RCVBUF, clientConfig.getReceiveBufferSize())
                .handler(new ChannelInitializer<SocketChannel>() {
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(
                                new NettyDecoder(),
                                clientHandler,
                                encoder);
                    }
                });
    }

    public void registerProcessor(final CommandType commandType, final NettyRequestProcessor processor, final ExecutorService executor) {
        this.clientHandler.registerProcessor(commandType, processor, executor);
    }

    public void send(final Address address, final Command command) throws RemotingException {
        final Channel channel = getChannel(address);
        if (channel == null) {
            throw new RemotingException("network error");
        }
        try {
            channel.writeAndFlush(command).addListener(new ChannelFutureListener(){
                public void operationComplete(ChannelFuture future) throws Exception {
                    if(future.isSuccess()){
                        logger.info("sent command {} to {}", command, address);
                    } else{
                        logger.error("send command {} to {} failed, error {}", command, address, future.cause());
                    }
                }
            });
        } catch (Exception ex) {
            String msg = String.format("send command %s to address %s encounter error", command, address);
            throw new RemotingException(msg, ex);
        }
    }

    public Channel getChannel(Address address) {
        Channel channel = channels.get(address);
        if(channel != null && channel.isActive()){
            return channel;
        }
        return createChannel(address, true);
    }

    public Channel createChannel(Address address, boolean isSync) {
        ChannelFuture future;
        try {
            synchronized (bootstrap){
                future = bootstrap.connect(new InetSocketAddress(address.getHost(), address.getPort()));
            }
            if(isSync){
                future.sync();
            }
            if (future.isSuccess()) {
                Channel channel = future.channel();
                channels.put(address, channel);
                return channel;
            }
        } catch (Exception ex) {
            logger.info("connect to {} error  {}", address, ex);
        }
        return null;
    }

    public ExecutorService getDefaultExecutor() {
        return defaultExecutor;
    }

    public void close() {
        try {
            closeChannels();
            if(workGroup != null){
                this.workGroup.shutdownGracefully();
            }
            if(defaultExecutor != null){
                defaultExecutor.shutdown();
            }
        } catch (Exception ex) {
            logger.error("netty client close exception", ex);
        }
        logger.info("netty client closed");
    }

    private void closeChannels(){
        for (Channel channel : this.channels.values()) {
            channel.close();
        }
        this.channels.clear();
    }

    public void removeChannel(Address address){
        Channel channel = this.channels.remove(address);
        if(channel != null){
            channel.close();
        }
    }
}
