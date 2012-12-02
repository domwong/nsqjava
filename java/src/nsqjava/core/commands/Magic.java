package nsqjava.core.commands;

public class Magic implements NSQCommand {

    @Override
    public String getCommandString() {
        return "  V2";
    }

    @Override
    public byte[] getCommandBytes() {
        return getCommandString().getBytes();
    }

}
