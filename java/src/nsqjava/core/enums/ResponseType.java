package nsqjava.core.enums;

public enum ResponseType {
    OK("OK"), INVALID("E_INVALID"), BAD_TOPIC("E_BAD_TOPIC"), BAD_CHANNEL("E_BAD_CHANNEL"), BAD_MESSAGE("E_BAD_MESSAGE"), PUT_FAILED("E_PUT_FAILED"), FINISH_FAILED("E_FINISH_FAILED"), REQUEUE_FAILED(
            "E_REQUEUE_FAILED"), CLOSE_WAIT("CLOSE_WAIT");

    private String code;

    private ResponseType(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }
    
}
