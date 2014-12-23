package com.petuum.ps.netty.protobuf;

import com.petuum.ps.common.Msg;
import com.petuum.ps.common.comm.BgWorkerHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;

/**
 * Created by Yuxin Su on 2014/12/4.
 */
public class BgWorker {

    private String host;
    private int port;
    private BgWorkerHandler handler;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ServerBootstrap server;
    private Bootstrap client;

    public BgWorker(String host, int port) {
        this.port = port;
        this.host = host;
        handler = new BgWorkerHandler(1);
        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();
    }

    public ChannelFuture setupServer() throws InterruptedException {
        server = new ServerBootstrap();
        server.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new ProtobufVarint32FrameDecoder());
                        pipeline.addLast(new ProtobufDecoder(Msg.AppConnectMsg.getDefaultInstance()));
                        pipeline.addLast(new ProtobufVarint32LengthFieldPrepender());
                        pipeline.addLast(new ProtobufEncoder());
                        pipeline.addLast(handler.new AppConnectHandler());
                    }
                })
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true);
        return server.bind(port).sync();
    }

    public ChannelFuture setupClient() throws InterruptedException {
        client = new Bootstrap();
        client.group(workerGroup)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new ProtobufVarint32FrameDecoder());
                        pipeline.addLast(new ProtobufDecoder(Msg.AppConnectMsg.getDefaultInstance()));
                        pipeline.addLast(new ProtobufVarint32LengthFieldPrepender());
                        pipeline.addLast(new ProtobufEncoder());
                        pipeline.addLast(handler.new AppConnectHandler());
                    }
                })
                .option(ChannelOption.SO_KEEPALIVE, true);
        return client.connect(host, port).sync();
    }

    public void run() throws InterruptedException {
        ChannelFuture f = setupServer();
        //ChannelFuture f2 = setupClient();
        System.out.println("ready to wait");
        //f2.channel().closeFuture().sync();
        f.channel().closeFuture().sync();
    }

    public static void main(String[] args) throws InterruptedException {
        int port = 8080;
        String host = "localhost";
        if(args.length > 1) {
            host = args[0];
            port = Integer.parseInt(args[1]);
        }

        new BgWorker(host, port).run();
    }
}
