package nsqjava.core.commands;

import java.math.BigInteger;

public class Finish implements NSQCommand {

    private final byte[] msgId;

    public Finish(byte[] msgId) {
        this.msgId = msgId;
    }

    @Override
    public String getCommandString() {
        BigInteger bi = new BigInteger(1, msgId);
        String hexString = String.format("%0" + (msgId.length << 1) + "X", bi);
        return "FIN " + new String(msgId) + "\n";
    }

    @Override
    public byte[] getCommandBytes() {
        return getCommandString().getBytes();
    }

}
