package org.apache.dolphinscheduler.remote;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.dolphinscheduler.remote.codec.NettyDecoder;
import org.apache.dolphinscheduler.remote.codec.NettyEncoder;
import org.apache.dolphinscheduler.remote.config.NettyServerConfig;
import org.apache.dolphinscheduler.remote.handler.NettyServerHandler;
import org.apache.dolphinscheduler.remote.utils.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author: Tboy
 */
public class NettyRemotingServer {

    private final Logger logger = LoggerFactory.getLogger(NettyRemotingServer.class);

    private final ServerBootstrap serverBootstrap = new ServerBootstrap();

    private final NettyEncoder encoder = new NettyEncoder();

    private final ExecutorService defaultExecutor = Executors.newFixedThreadPool(Constants.CPUS);

    private final NioEventLoopGroup bossGroup;

    private final NioEventLoopGroup workGroup;

    private final NettyServerConfig serverConfig;

    public NettyRemotingServer(final NettyServerConfig serverConfig){
        this.serverConfig = serverConfig;

        this.bossGroup = new NioEventLoopGroup(1, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);

            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("NettyServerBossThread_%d", this.threadIndex.incrementAndGet()));
            }
        });

        this.workGroup = new NioEventLoopGroup(serverConfig.getWorkerThread(), new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);

            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("NettyServerWorkerThread_%d", this.threadIndex.incrementAndGet()));
            }
        });
    }

    public void start(){

        this.serverBootstrap
                .group(this.bossGroup, this.workGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_BACKLOG, serverConfig.getSoBacklog())
                .option(ChannelOption.SO_KEEPALIVE, serverConfig.isSoKeepalive())
                .option(ChannelOption.TCP_NODELAY, serverConfig.isTcpNodelay())
                .option(ChannelOption.SO_SNDBUF, serverConfig.getSendBufferSize())
                .option(ChannelOption.SO_RCVBUF, serverConfig.getReceiveBufferSize())
                .childHandler(new ChannelInitializer<NioSocketChannel>() {

                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        initChannel(ch);
                    }
                });

        ChannelFuture future = null;
        try {
            future = serverBootstrap.bind(serverConfig.getListenPort()).sync();
        } catch (Exception e) {
            logger.error("NettyRemotingServer bind fail {}, exit", e);
            throw new RuntimeException(String.format("NettyRemotingServer bind %s fail", serverConfig.getListenPort()));
        }
        if (future.isSuccess()) {
            logger.info("NettyRemotingServer bind success at port : {}", serverConfig.getListenPort());
        } else if (future.cause() != null) {
            logger.error("NettyRemotingServer bind fail {}", future.cause());
        } else {
            throw new RuntimeException(String.format("NettyRemotingServer bind %s fail", serverConfig.getListenPort()));
        }
    }

    private void initChannel(NioSocketChannel ch) throws Exception{
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast("encoder", encoder);
        pipeline.addLast("decoder", new NettyDecoder());
        pipeline.addLast("handler", new NettyServerHandler(this));
    }

    public ExecutorService getDefaultExecutor() {
        return defaultExecutor;
    }

    public void close() {
        try {
            if(bossGroup != null){
                this.bossGroup.shutdownGracefully();
            }
            if(workGroup != null){
                this.workGroup.shutdownGracefully();
            }
            if(defaultExecutor != null){
                defaultExecutor.shutdown();
            }
        } catch (Exception ex) {
            logger.error("netty server close exception", ex);
        }
        logger.info("netty server closed");
    }
}
