package nsqjava.core;

import nsqjava.core.commands.Finish;
import nsqjava.core.commands.Magic;
import nsqjava.core.commands.NSQCommand;
import nsqjava.core.enums.ResponseType;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NSQChannelHandler extends SimpleChannelHandler {

    private static final Logger log = LoggerFactory.getLogger(NSQChannelHandler.class);

    private boolean authenticated = false;

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        log.debug("Received message " + e.getMessage());
        Object o = e.getMessage();
        if (o instanceof ResponseType) {

            if (o == ResponseType.HEARTBEAT) {
                log.debug("received heartbeat");
            } else {
                log.debug("received " + o);
            }
        } else if (o instanceof NSQFrame) {
            log.debug("received nsqframe");
            NSQFrame frm = (NSQFrame) o;
            // do stuff with it.
            e.getChannel().write(new Finish(frm.getMsg().getMessageId()));
        } else {
            log.debug("something else " + o);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        e.getCause().printStackTrace();
        e.getChannel().close();
    }

    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        // TODO Auto-generated method stub
        super.channelDisconnected(ctx, e);
        this.authenticated = false;
    }

    @Override
    public void writeRequested(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        Object o = e.getMessage();
        if (o instanceof NSQFrame) {
            log.debug("NSQFrame writing");
            if (!authenticated) {
                // TODO - should probably queue this stuff up rather than throw exception.
                e.getFuture().setFailure(new Exception("Not authenticated yet"));
                return;
            }
            NSQFrame frame = (NSQFrame) e.getMessage();

            ChannelBuffer buf = ChannelBuffers.buffer(frame.getSize());
            buf.writeBytes(frame.getBytes());

            Channels.write(ctx, e.getFuture(), buf);

        } else if (o instanceof NSQCommand) {
            log.debug("NSQCommand writing "+o);
            if (!authenticated && !(o instanceof Magic)) {
                // TODO - should probably queue this stuff up rather than throw exception.
                e.getFuture().setFailure(new Exception("Not authenticated yet"));
                return;

            }
            NSQCommand cmd = (NSQCommand) e.getMessage();
            byte[] bytes = cmd.getCommandBytes();
            ChannelBuffer buf = ChannelBuffers.buffer(bytes.length);
            buf.writeBytes(bytes);

            Channels.write(ctx, e.getFuture(), buf);

        } else {
            log.error("Unknown message type " + o);
        }

    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        log.debug("Channel connected");
        ChannelFuture future = e.getChannel().write(new Magic());
        future.addListener(new ChannelFutureListener() {

            @Override
            public void operationComplete(ChannelFuture arg0) throws Exception {
                if (arg0.isSuccess()) {
                    log.debug("Authenticated");
                    authenticated = true;
                } else if (arg0.isCancelled()) {
                    log.debug("CANCELLED");
                } else if (arg0.isDone()) {
                    log.debug("DONE");
                }
            }
        });

    }

}
