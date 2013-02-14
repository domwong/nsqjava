package org.nsqjava.examples;

import java.net.InetSocketAddress;


import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.nsqjava.core.NSQChannelHandler;
import org.nsqjava.core.NSQFrameDecoder;
import org.nsqjava.core.commands.Publish;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExamplePublisher {

    private static final Logger log = LoggerFactory.getLogger(ExamplePublisher.class);

    public static void main(String[] args) throws Exception {
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        log.info("Connecting to " + host + ":" + port);

        // connect to nsqd TODO add step for lookup via nsqlookupd
        ChannelFactory factory = new NioClientSocketChannelFactory();

        final ClientBootstrap bootstrap = new ClientBootstrap(factory);
        final NSQChannelHandler nsqhndl = new NSQChannelHandler(bootstrap);

        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {

            public ChannelPipeline getPipeline() {
                return Channels.pipeline(new NSQFrameDecoder(), nsqhndl);
            }
        });
        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("keepAlive", true);
        bootstrap.setOption("remoteAddress", new InetSocketAddress(host, port));
        ChannelFuture future = bootstrap.connect();

        future.awaitUninterruptibly();
        if (!future.isSuccess()) {
            future.getCause().printStackTrace();
            return;
        }

        Thread.currentThread().sleep(5000);
        log.debug("Now to do some work");
        Channel chan = future.getChannel();
        for (int i = 1; i < 1000; ++i) {
            Publish pub = new Publish("newtopic", ("Hello World " + i).getBytes());
            log.debug("publishing to" + pub.getCommandString());
            chan.write(pub);
        }
        log.debug("Close wait1");
        nsqhndl.close().awaitUninterruptibly();
        log.debug("Close wait2");
        factory.releaseExternalResources();
    }

}
