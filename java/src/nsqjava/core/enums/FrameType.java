package nsqjava.core.enums;

public enum FrameType {
    RESPONSE(0), ERROR(1), MESSAGE(2);

    private int code;

    private FrameType(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
