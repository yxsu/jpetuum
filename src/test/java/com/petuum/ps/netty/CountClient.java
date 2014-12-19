package com.petuum.ps.netty;

import com.petuum.ps.netty.protobuf.CountClientHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * Created by Yuxin Su on 2014/12/4.
 */
public class CountClient {

    private String host;
    private int port;

    public CountClient(String host, int port) {
        this.port = port;
        this.host = host;
    }

    public void run() throws InterruptedException {
        EventLoopGroup worker = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            // For client side, there is only Bootstrap rather than
            // ServerBootstrap in server side
            b.group(worker)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        //There is no "child" concept
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(new CountClientHandler());
                        }
                    })
                    .option(ChannelOption.SO_KEEPALIVE, true);
            ChannelFuture f = b.connect(host, port).sync();
            f.channel().closeFuture().sync();
        } finally {
            worker.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        int port = 8080;
        String host = "localhost";
        if(args.length > 1) {
            host = args[0];
            port = Integer.parseInt(args[1]);
        }

        new CountClient(host, port).run();
    }
}
