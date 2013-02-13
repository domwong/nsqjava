package nsqjava.core;

import java.nio.ByteBuffer;

import nsqjava.core.commands.Nop;
import nsqjava.core.enums.FrameType;
import nsqjava.core.enums.ResponseType;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NSQFrameDecoder extends FrameDecoder {
    
    private static final Logger log = LoggerFactory.getLogger(NSQFrameDecoder.class);
    
    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel chan, ChannelBuffer buff) throws Exception {
        int readableBytes = buff.readableBytes();
        if (readableBytes < 4) {
            log.debug("not enough readable bytes");
            return null;
        }
        buff.markReaderIndex();
        int size = buff.readInt();
        if (readableBytes <  size) {
            log.debug("still not enough readable bytes");
            buff.resetReaderIndex();
            return null;
        }
        ChannelBuffer cbuf = ChannelBuffers.buffer(size);
        buff.readBytes(cbuf);
        int frameType = cbuf.readInt();
        FrameType t = FrameType.fromCode(frameType);
        log.debug("Frame type is "+ frameType +" " + t+ " size " + size + " readablebytes " + readableBytes );
        if (t ==null) {
            // uh oh
            throw new Exception("Unknown frame type "+frameType);
        }
        switch (t) {
        case ERROR:
            return handleError(cbuf, size);
        case RESPONSE:
            return handleResponse(chan,cbuf, size);
        case MESSAGE:
            return handleMessage(chan, cbuf, size);
        }
        return null;

    }
    
    private NSQFrame handleMessage(Channel chan, ChannelBuffer buff , int size) {
        long ts = buff.readLong();
        int attempts = buff.readUnsignedShort();
        byte[] msgId = new byte[16];
        buff.readBytes(msgId);
        byte[] body = new byte[size - 4 - NSQMessage.MIN_SIZE_BYTES];
        buff.readBytes(body);
        NSQMessage msg = new NSQMessage(ts, attempts, msgId, body);
        NSQFrame frame = new NSQFrame(FrameType.MESSAGE, size, msg);
        log.debug("decoded message with id "+ new String(msgId));
        return frame;
    }
    
    private String handleError(ChannelBuffer buff, int size) {
        return readString(buff, size);
    }

    private ResponseType handleResponse(Channel chan, ChannelBuffer buff, int size) {
        String resp = readString(buff, size);
        if (resp != null ) {
            log.debug("handled response with name "+resp);
        }
        ResponseType t = ResponseType.fromCode(resp);
        if ("_heartbeat_".equals(resp)) {
            sendNOP(chan);
            return ResponseType.HEARTBEAT;
        }
        return t;
    }

    private String readString(ChannelBuffer buff, int size) {
        ByteBuffer bb = ByteBuffer.allocate(size - 4);
        buff.readBytes(bb);
        String resp = new String(bb.array());
        return resp;
    }

    private void sendNOP(Channel chan) {
        log.debug("Sending NOP");
        chan.write(new Nop());

    }
}
