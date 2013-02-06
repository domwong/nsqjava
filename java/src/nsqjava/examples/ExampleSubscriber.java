package nsqjava.examples;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import nsqjava.core.NSQChannelHandler;
import nsqjava.core.NSQFrame;
import nsqjava.core.NSQFrameDecoder;
import nsqjava.core.commands.Finish;
import nsqjava.core.commands.Ready;
import nsqjava.core.commands.Requeue;
import nsqjava.core.commands.Subscribe;
import nsqjava.core.enums.ResponseType;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExampleSubscriber {

    private static final Logger log = LoggerFactory.getLogger(ExampleSubscriber.class);

    public static void main(String[] args) throws Exception {
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        log.info("Connecting to " + host + ":" + port);

        // connect to nsqd TODO add step for lookup via nsqlookupd
        ChannelFactory factory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());

        final ClientBootstrap bootstrap = new ClientBootstrap(factory);
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() {
                return Channels.pipeline(new NSQFrameDecoder(), new ExampleHandler(bootstrap));
            }

        });
        
        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("keepAlive", true);
        InetSocketAddress addr = new InetSocketAddress(host, port);
        bootstrap.setOption("remoteAddress", addr);
        ChannelFuture future = bootstrap.connect(addr);
        future.sync();
        Thread.currentThread().sleep(5000);
        log.debug("Now to do some work");
        Channel chan = future.getChannel();

        Subscribe sub = new Subscribe("newtopic", "CHANNELFOO", "SUBSCRIBERSHORT", "SUBSCRIBERLONG");
        log.debug("Subscribing to " + sub.getCommandString());
        chan.write(sub);
        Ready rdy = new Ready(100);
        chan.write(rdy);

        future.awaitUninterruptibly();
        if (!future.isSuccess()) {
            future.getCause().printStackTrace();
        }
        future.getChannel().getCloseFuture().awaitUninterruptibly();
        
    }

    public static class ExampleHandler extends NSQChannelHandler {

        public ExampleHandler(ClientBootstrap bs) {
            super(bs);
        }
        
        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
            log.debug("Received message " + e.getMessage());
            Object o = e.getMessage();
            if (o instanceof NSQFrame) {
                log.debug("received nsqframe");
                NSQFrame frm = (NSQFrame) o;
                try {
                    // do stuff with it.
                    log.debug("Received message\n" + new String(frm.getMsg().getBody()));
                    // once done, confirm with server
                    log.debug("Confirming message\n" + new String(frm.getMsg().getMessageId()));
                    ChannelFuture future = e.getChannel().write(new Finish(frm.getMsg().getMessageId()));
                    future.sync();
                    log.debug("RDY again");
                    e.getChannel().write(new Ready(100)).sync();
                } catch (Exception ex) {
                    log.error("Failed to process message due to exception", ex);
                    // failed to process message, requeue
                    e.getChannel().write(new Requeue(frm.getMsg().getMessageId(), 1));
                }
            } else if (o instanceof ResponseType) {
                // don't care
            } else {
                // 
            }
        }
        
    }

}
