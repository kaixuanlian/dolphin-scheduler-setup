package org.apache.dolphinscheduler.remote.handler;

import io.netty.channel.*;
import org.apache.dolphinscheduler.remote.command.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: Tboy
 */
@ChannelHandler.Sharable
public class NettyClientHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyClientHandler.class);

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        //TODO
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        //TODO
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOGGER.error("exceptionCaught", cause);
        ctx.close();
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        Channel ch = ctx.channel();
        ChannelConfig config = ch.config();

        if (!ch.isWritable()) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("{} is not writable, over high water level : {}",
                        new Object[]{ch, config.getWriteBufferHighWaterMark()});
            }

            config.setAutoRead(false);
        } else {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("{} is writable, to low water : {}",
                        new Object[]{ch, config.getWriteBufferLowWaterMark()});
            }
            config.setAutoRead(true);
        }
    }
}