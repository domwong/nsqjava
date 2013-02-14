package org.nsqjava.core.enums;

public enum CommandType {
    SUBSCRIBE("SUB"), PUBLISH("PUB"), READY("RDY"), FINISH("FIN"), REQUEUE("REQ"), CLOSE("CLS"), NOP("NOP");

    private String code;

    private CommandType(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }
}
