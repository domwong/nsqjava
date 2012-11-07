package nsqjava.core.commands;

import nsqjava.core.enums.CommandType;

public class Ready implements NSQCommand {

    private int count;

    public Ready(int count) {
        this.count = count;
    }

    @Override
    public String getCommandString() {
        return String.format("%s %s", CommandType.READY, count);
    }

}
