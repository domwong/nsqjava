package nsqjava.core;

import java.nio.ByteBuffer;

import nsqjava.core.enums.FrameType;

public class NSQFrame {
    private final FrameType frameId;
    private final int size;
    private final NSQMessage msg;

    public NSQFrame(FrameType type, int size, NSQMessage msg) {
        this.frameId = type;
        this.size = size;
        this.msg = msg;
    }

    public FrameType getFrameId() {
        return frameId;
    }

    public int getSize() {
        return size;
    }

    public NSQMessage getMsg() {
        return msg;
    }

    public byte[] getBytes(){
        ByteBuffer bb = ByteBuffer.allocate(size);
        bb.putInt(size);
        bb.putInt(frameId.getCode());
        bb.put(msg.getBytes());
        return bb.array(); 
    }
}
