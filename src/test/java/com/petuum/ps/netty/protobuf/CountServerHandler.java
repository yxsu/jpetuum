package com.petuum.ps.netty.protobuf;

import com.petuum.ps.netty.protobuf.AddressBookProtos;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.util.ReferenceCountUtil;

/**
 * Created by Yuxin Su on 2014/12/2.
 */
public class CountServerHandler extends SimpleChannelInboundHandler<AddressBookProtos.Person> {
    private int local_count = 0;

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("channelActive");
        ctx.writeAndFlush(AddressBookProtos.Person.newBuilder()
                .setId(0)
                .setName("yxsu")
                .setEmail("yxsu@cse").build());
        System.out.println("Write sucessfully");
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }

    @Override
    protected void messageReceived(ChannelHandlerContext ctx, AddressBookProtos.Person person) throws Exception {
        if(person.getId() % 10000 == 0) {
            System.out.println("Server : " + String.valueOf(person.getId()));
        }
        ctx.writeAndFlush(person);
    }
}
