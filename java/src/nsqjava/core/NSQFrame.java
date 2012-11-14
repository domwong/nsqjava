package nsqjava.core;

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
    
    
}
