package nsqjava.core;

import nsqjava.core.enums.FrameType;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

public class NSQFrameDecoder extends FrameDecoder {

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel chan, ChannelBuffer buff) throws Exception {
        int readableBytes = buff.readableBytes();
        if (readableBytes < 8) {
            return null;
        }
        int size = buff.getInt(0);
        int frameType = buff.getInt(3);
        FrameType t = FrameType.fromCode(frameType);
        if (8 + size > readableBytes) {
            // not enough data
            return null;
        }
        buff.skipBytes(8);
        long ts = buff.readLong();
        int attempts = buff.readUnsignedShort();
        byte[] msgId = new byte[16];
        buff.readBytes(msgId);
        byte[] body = new byte[size - NSQMessage.MIN_SIZE_BYTES];
        buff.readBytes(body);
        NSQMessage msg = new NSQMessage(ts, attempts, msgId, body);
        NSQFrame frame = new NSQFrame(t, size, msg);
        return frame;
        
    }

}
