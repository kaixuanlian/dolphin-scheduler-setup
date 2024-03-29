package org.apache.dolphinscheduler.remote.handler;

import io.netty.channel.*;
import org.apache.dolphinscheduler.remote.NettyRemotingClient;
import org.apache.dolphinscheduler.remote.command.Command;
import org.apache.dolphinscheduler.remote.command.CommandType;
import org.apache.dolphinscheduler.remote.processor.NettyRequestProcessor;
import org.apache.dolphinscheduler.remote.utils.ChannelUtils;
import org.apache.dolphinscheduler.remote.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

/**
 * @Author: Tboy
 */
@ChannelHandler.Sharable
public class NettyClientHandler extends ChannelInboundHandlerAdapter {

    private final Logger logger = LoggerFactory.getLogger(NettyClientHandler.class);

    private final NettyRemotingClient nettyRemotingClient;

    private final ConcurrentHashMap<CommandType, Pair<NettyRequestProcessor, ExecutorService>> processors = new ConcurrentHashMap();

    public NettyClientHandler(NettyRemotingClient nettyRemotingClient){
        this.nettyRemotingClient = nettyRemotingClient;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        nettyRemotingClient.removeChannel(ChannelUtils.toAddress(ctx.channel()));
        ctx.channel().close();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        processReceived(ctx.channel(), (Command)msg);
    }

    public void registerProcessor(final CommandType commandType, final NettyRequestProcessor processor, final ExecutorService executor) {
        ExecutorService executorRef = executor;
        if(executorRef == null){
            executorRef = nettyRemotingClient.getDefaultExecutor();
        }
        this.processors.putIfAbsent(commandType, new Pair<NettyRequestProcessor, ExecutorService>(processor, executorRef));
    }

    private void processReceived(final Channel channel, final Command msg) {
        final CommandType commandType = msg.getType();
        final Pair<NettyRequestProcessor, ExecutorService> pair = processors.get(commandType);
        if (pair != null) {
            Runnable r = new Runnable() {
                public void run() {
                    try {
                        pair.getLeft().process(channel, msg);
                    } catch (Throwable ex) {
                        logger.error("process msg {} error : {}", msg, ex);
                    }
                }
            };
            try {
                pair.getRight().submit(r);
            } catch (RejectedExecutionException e) {
                logger.warn("thread pool is full, discard msg {} from {}", msg, ChannelUtils.getRemoteAddress(channel));
            }
        } else {
            logger.warn("commandType {} not support", commandType);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("exceptionCaught", cause);
        nettyRemotingClient.removeChannel(ChannelUtils.toAddress(ctx.channel()));
        ctx.channel().close();
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        Channel ch = ctx.channel();
        ChannelConfig config = ch.config();

        if (!ch.isWritable()) {
            if (logger.isWarnEnabled()) {
                logger.warn("{} is not writable, over high water level : {}",
                        new Object[]{ch, config.getWriteBufferHighWaterMark()});
            }

            config.setAutoRead(false);
        } else {
            if (logger.isWarnEnabled()) {
                logger.warn("{} is writable, to low water : {}",
                        new Object[]{ch, config.getWriteBufferLowWaterMark()});
            }
            config.setAutoRead(true);
        }
    }
}