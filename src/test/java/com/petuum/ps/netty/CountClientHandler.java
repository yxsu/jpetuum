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
        ByteBuf buf = (ByteBuf)msg;
        int count = buf.readInt();
        if(count % 10000 == 0) {
            System.out.println("Client : " + String.valueOf(count));
        }
        buf.writeInt(count + 1);
        ctx.writeAndFlush(buf);
    }
}
