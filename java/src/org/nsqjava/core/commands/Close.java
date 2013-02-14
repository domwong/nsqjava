package org.nsqjava.core.commands;

public class Close implements NSQCommand {

    @Override
    public String getCommandString() {
        return "CLS\n";
    }

    @Override
    public byte[] getCommandBytes() {
        return getCommandString().getBytes();
    }

}
