package com.petuum.ps.netty.protobuf;

import com.petuum.ps.netty.protobuf.AddressBookProtos;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.Date;

/**
 * Created by Yuxin Su on 2014/12/4.
 */
public class CountClientHandler extends SimpleChannelInboundHandler<AddressBookProtos.Person> {

    @Override
    protected void messageReceived(ChannelHandlerContext ctx, AddressBookProtos.Person person) throws Exception {
        AddressBookProtos.Person.Builder newPerson = AddressBookProtos.Person.newBuilder();
        newPerson.setId(person.getId() + 1);
        newPerson.setName("yxsu");
        newPerson.setEmail("yxsu@cse");
        if(newPerson.getId() % 10000 == 0) {
            System.out.println("Client : " + String.valueOf(newPerson.getId()));
        }
        if(newPerson.getId() < 1000000) {
            ctx.writeAndFlush(newPerson.build());
        } else {
            ctx.close();
        }
    }
}
