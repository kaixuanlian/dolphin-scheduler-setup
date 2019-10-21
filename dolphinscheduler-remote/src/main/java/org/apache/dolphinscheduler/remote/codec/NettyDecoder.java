package org.apache.dolphinscheduler.remote.codec;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import org.apache.dolphinscheduler.remote.command.Command;
import org.apache.dolphinscheduler.remote.command.CommandHeader;

import java.util.List;

/**
 * @Author: Tboy
 */
public class NettyDecoder extends ReplayingDecoder<NettyDecoder.State> {

    public NettyDecoder(){
        super(State.MAGIC);
    }

    private final CommandHeader commandHeader = new CommandHeader();

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        switch (state()){
            case MAGIC:
                checkMagic(in.readByte());
                checkpoint(State.VERSION);
            case VERSION:
                checkVersion(in.readByte());
                checkpoint(State.COMMAND);
            case COMMAND:
                commandHeader.setCmd(in.readByte());
                checkpoint(State.OPAQUE);
            case OPAQUE:
                commandHeader.setOpaque(in.readLong());
                checkpoint(State.BODY_LENGTH);
            case BODY_LENGTH:
                commandHeader.setBodyLength(in.readInt());
                checkpoint(State.BODY);
            case BODY:
                byte[] body = new byte[commandHeader.getBodyLength()];
                in.readBytes(body);
                //
                Command packet = new Command();
                packet.setCmd(commandHeader.getCmd());
                packet.setOpaque(commandHeader.getOpaque());
                packet.setBody(Unpooled.wrappedBuffer(body));
                out.add(packet);
                //
                checkpoint(State.MAGIC);
        }
    }

    private void checkMagic(byte magic) {
        if (magic != Command.MAGIC) {
            throw new IllegalArgumentException("illegal packet [magic]" + magic);
        }
    }

    private void checkVersion(byte version) {
        if (version != Command.VERSION) {
            throw new IllegalArgumentException("illegal packet [version]" + version);
        }
    }

    enum State{
        MAGIC,
        VERSION,
        COMMAND,
        OPAQUE,
        BODY_LENGTH,
        BODY;
    }
}
