package nsqjava.core;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import nsqjava.core.commands.Finish;
import nsqjava.core.commands.NSQCommand;
import nsqjava.core.commands.Publish;
import nsqjava.core.enums.ResponseType;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientHandler extends SimpleChannelHandler {

    private static final Logger log = LoggerFactory.getLogger(ClientHandler.class);

    private boolean authenticated = false;
    private Channel channel;

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        //NSQFrame m = (NSQFrame) e.getMessage();
        log.debug("Received message " + e.getMessage());
        Object o = e.getMessage();
        if (o instanceof ResponseType ) {
            
            if(o == ResponseType.HEARTBEAT) {
                log.debug("received heartbeat");
            } else {
                log.debug("received "+o);
            }
        } else if (o instanceof NSQFrame) {            
            log.debug("received nsqframe");
            NSQFrame frm = (NSQFrame) o;
            // do stuff with it.
            ctx.getChannel().write(new Finish(frm.getMsg().getMessageId()));
        } else {
            log.debug("something else "+o);
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
        log.debug("Writing stuff");
        // 
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
            log.debug("NSQCommand writing");
            if (!authenticated) {
                // TODO - should probably queue this stuff up rather than throw exception.
                e.getFuture().setFailure(new Exception("Not authenticated yet"));
                return;
            }
            NSQCommand cmd = (NSQCommand) e.getMessage();
            log.debug("Trying to write stuff "+cmd.getCommandString());
            byte[] bytes = cmd.getCommandBytes();
            ChannelBuffer buf = ChannelBuffers.buffer(bytes.length);
            buf.writeBytes(bytes);
            
            Channels.write(ctx, e.getFuture(), buf);
            
        } else {
            // assume it's a authenication attempt
            super.writeRequested(ctx, e);

        }

    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        log.debug("Channel connected");
        ChannelBuffer buf = ChannelBuffers.buffer(4);
        buf.writeBytes("  V2".getBytes());
        ChannelFuture future = e.getChannel().write(buf);
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

    public static void main(String[] args) throws Exception {
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        log.info("Connecting to " + host + ":" + port);

        // connect to nsqd TODO add step for lookup via nsqlookupd
        ChannelFactory factory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());

        ClientBootstrap bootstrap = new ClientBootstrap(factory);
        final ClientHandler handler = new ClientHandler();
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() {
                return Channels.pipeline( new NSQFrameDecoder(), handler);
            }
        });

        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("keepAlive", true);
        ChannelFuture future = bootstrap.connect(new InetSocketAddress(host, port));
        future.sync();
        Thread.currentThread().sleep(5000);
        log.debug("Now to do some work");
        handler.setChannel(future.getChannel());

//        Subscribe sub = new Subscribe("TOPICFOO", "CHANNELFOO", "SHORTID", "LONGERID");
//        log.debug("Subscribing to " + sub.getCommandString());
//        handler.getChannel().write(sub);
//        Ready rdy = new Ready(10);
//        handler.getChannel().write(rdy);
        Publish pub = new Publish("TOPICFOO", "FINally workign <alskdjla>".getBytes());
        log.debug("publishing to" + pub.getCommandString());
        handler.getChannel().write(pub);

        future.awaitUninterruptibly();
        if (!future.isSuccess()) {
            future.getCause().printStackTrace();
        }
        future.getChannel().getCloseFuture().awaitUninterruptibly();
        factory.releaseExternalResources();
    }

    public Channel getChannel() {
        return this.channel;
    }

    public void setChannel(Channel chan) {
        this.channel = chan;
    }
}
