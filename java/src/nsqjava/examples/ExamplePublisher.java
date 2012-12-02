package nsqjava.examples;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import nsqjava.core.NSQChannelHandler;
import nsqjava.core.NSQFrameDecoder;
import nsqjava.core.commands.Publish;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExamplePublisher {

    private static final Logger log = LoggerFactory.getLogger(ExamplePublisher.class);

    public static void main(String[] args) throws Exception {
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        log.info("Connecting to " + host + ":" + port);

        // connect to nsqd TODO add step for lookup via nsqlookupd
        ChannelFactory factory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());

        ClientBootstrap bootstrap = new ClientBootstrap(factory);
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() {
                return Channels.pipeline(new NSQFrameDecoder(), new NSQChannelHandler());
            }
        });

        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("keepAlive", true);
        ChannelFuture future = bootstrap.connect(new InetSocketAddress(host, port));
        future.sync();
        Thread.currentThread().sleep(5000);
        log.debug("Now to do some work");
        Channel chan = future.getChannel();

        for (int i = 1; i < 1000; ++i) {
            Publish pub = new Publish("newtopic", ("BRAVE NEW WOWLD this is going to be bigger than what you expect " + Integer.toString(i)).getBytes());
            log.debug("publishing to" + pub.getCommandString());
            chan.write(pub);
        }
        future.awaitUninterruptibly();
        if (!future.isSuccess()) {
            future.getCause().printStackTrace();
        }
        future.getChannel().getCloseFuture().awaitUninterruptibly();
        factory.releaseExternalResources();
    }

}
