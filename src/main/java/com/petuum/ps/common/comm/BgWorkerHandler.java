package com.petuum.ps.common.comm;

import com.petuum.ps.common.Msg;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by Yuxin Su on 2014/12/23.
 */
public class BgWorkerHandler {
    private static Logger log = LogManager.getLogger(BgWorkerHandler.class);
    private int id;

    public BgWorkerHandler(int id) {
        this.id = id;
    }
    public class AppConnectHandler extends SimpleChannelInboundHandler<Msg.AppConnectMsg> {

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            System.out.println("Channel is active");
            Msg.AppConnectMsg msg = Msg.AppConnectMsg.newBuilder()
                    .setSenderID(id)
                    .setReceiverID(0).build();
            ctx.writeAndFlush(msg);
        }

        @Override
        protected void messageReceived(ChannelHandlerContext ctx, Msg.AppConnectMsg msg) throws Exception {
            System.out.println("BgWorker received AppConnectMsg from " + String.valueOf(msg.getSenderID()));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            cause.printStackTrace();
            ctx.close();
        }
    }
}
