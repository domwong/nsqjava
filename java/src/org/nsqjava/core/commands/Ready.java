package org.nsqjava.core.commands;

import org.nsqjava.core.enums.CommandType;

public class Ready implements NSQCommand {

    private int count;

    public Ready(int count) {
        this.count = count;
    }

    @Override
    public String getCommandString() {
        return String.format("%s %s\n", CommandType.READY.getCode(), count);
    }

    @Override
    public byte[] getCommandBytes() {
        return getCommandString().getBytes();
    }

}
