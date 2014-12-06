package com.petuum.ps.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.ReferenceCountUtil;

/**
 * Created by Yuxin Su on 2014/12/2.
 */
public class CountServerHandler extends ChannelHandlerAdapter {
    private int local_count = 0;

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("channelActive");
        MyMessage msg = new MyMessage(local_count, "");
        ctx.writeAndFlush(msg);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        MyMessage buf = (MyMessage)msg;


            local_count = buf.count;
            if(local_count % 10000 == 0) {
                System.out.println("Server : " + String.valueOf(local_count));
            }
            buf.count = local_count;
            ctx.writeAndFlush(buf);

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}
