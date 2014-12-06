package com.petuum.ps.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

import java.util.Date;

/**
 * Created by Yuxin Su on 2014/12/4.
 */
public class CountClientHandler extends ChannelHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        MyMessage buf = (MyMessage)msg;
        int count = buf.count;
        if(count % 10000 == 0) {
            System.out.println("Client : " + String.valueOf(count));
        }
        if(count < 1000000) {
            buf.count = count + 1;
            ctx.writeAndFlush(buf);
        } else {
            ctx.close();
        }
    }
}
