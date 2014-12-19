package com.petuum.ps.netty.protobuf;

import com.petuum.ps.netty.protobuf.AddressBookProtos.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;

import java.io.IOException;


/**
 * Created by Yuxin Su on 2014/12/4.
 */
public class LocalCount {
    public static void main(String[] args) throws InterruptedException, IOException {

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
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(new ProtobufVarint32FrameDecoder());
                            pipeline.addLast(new ProtobufDecoder(Person.getDefaultInstance()));
                            pipeline.addLast(new ProtobufVarint32LengthFieldPrepender());
                            pipeline.addLast(new ProtobufEncoder());
                            pipeline.addLast(new CountServerHandler());
                        }
                    });
            //client
            Bootstrap b = new Bootstrap();
            b.group(clientGroup)
                    .channel(LocalChannel.class)
                    .handler(new ChannelInitializer<LocalChannel>() {
                        @Override
                        protected void initChannel(LocalChannel ch) throws Exception {
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(new ProtobufVarint32FrameDecoder());
                            pipeline.addLast(new ProtobufDecoder(Person.getDefaultInstance()));
                            pipeline.addLast(new ProtobufVarint32LengthFieldPrepender());
                            pipeline.addLast(new ProtobufEncoder());
                            pipeline.addLast(new CountClientHandler());
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
