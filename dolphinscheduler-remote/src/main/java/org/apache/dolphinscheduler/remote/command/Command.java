package org.apache.dolphinscheduler.remote.command;

import io.netty.buffer.ByteBuf;

import java.io.Serializable;

/**
 *
 *  * *************************************************************************
 *                                   Protocol
 *  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
 *       1   │    1    │    1    │     8         |       4       |                |
 *  ├ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
 *           │         │         │               │               |                |
 *  │  magic    cmd       opaque     bodyLength      |   body value    |
 *           │         │         │               │               |                |
 *  └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
 * @Author: Tboy
 */
public class Command implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final byte MAGIC = (byte) 0xbabe;

    public Command(){
    }

    public Command(long opaque){
        this.opaque = opaque;
    }

    private CommandType type;

    private long opaque;

    private ByteBuf body;

    public CommandType getType() {
        return type;
    }

    public void setType(CommandType type) {
        this.type = type;
    }

    public long getOpaque() {
        return opaque;
    }

    public void setOpaque(long opaque) {
        this.opaque = opaque;
    }

    public ByteBuf getBody() {
        return body;
    }

    public void setBody(ByteBuf body) {
        this.body = body;
    }

    public boolean isBodyEmtpy(){
        return this.body == null || this.body.readableBytes() == 0;
    }

    public int getBodyLength(){
        return this.body.readableBytes();
    }

    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (opaque ^ (opaque >>> 32));
        return result;
    }

    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Command other = (Command) obj;
        if (opaque != other.opaque)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "Command [type=" + type + ", opaque=" + opaque + ", bodyLen=" + (body == null ? 0 : body.readableBytes()) + "]";
    }

}
