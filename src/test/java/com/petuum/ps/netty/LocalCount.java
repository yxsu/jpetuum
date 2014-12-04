package com.petuum.ps.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;


/**
 * Created by Yuxin Su on 2014/12/4.
 */
public class LocalCount {
    public static void main(String[] args) throws InterruptedException {

        LocalAddress local = new LocalAddress("8089");

        EventLoopGroup serverGroup = new DefaultEventLoopGroup();
        EventLoopGroup clientGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap sb = new ServerBootstrap();
            sb.group(serverGroup)
                    .channel(LocalServerChannel.class)
                    .handler(new ChannelInitializer<LocalServerChannel>(){

                        @Override
                        protected void initChannel(LocalServerChannel ch) throws Exception {

                        }
                    })
                    .childHandler(new ChannelInitializer<LocalChannel>() {
                        @Override
                        protected void initChannel(LocalChannel ch) throws Exception {
                            ch.pipeline().addLast(new CountServerHandler());
                        }
                    });
            //client
            Bootstrap b = new Bootstrap();
            b.group(clientGroup)
                    .channel(LocalChannel.class)
                    .handler(new ChannelInitializer<LocalChannel>() {
                        @Override
                        protected void initChannel(LocalChannel ch) throws Exception {
                            ch.pipeline().addLast(new CountClientHandler());
                        }
                    });
            //start the server
            ChannelFuture serverFuture = sb.bind(local).sync();
            ChannelFuture clientFuture = b.connect(local).sync();
            clientFuture.channel().closeFuture().sync();
            serverFuture.channel().closeFuture().sync();
        } finally {
            serverGroup.shutdownGracefully();
            clientGroup.shutdownGracefully();
        }
    }
}
