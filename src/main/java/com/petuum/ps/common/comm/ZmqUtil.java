package com.petuum.ps.common.comm;

import com.petuum.ps.common.util.IntBox;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.nio.ByteBuffer;

/**
 * Created by admin on 2014/8/11.
 */
public class ZmqUtil {
    public static final int ZMQ_IDENTITY = 0;
    public static final int ZMQ_ROUTER_MANDATORY = 1;
    public static final int ZMQ_SNDBUF = 2;
    public static final int ZMQ_RCVBUF = 3;


    public static int entityID2ZmqID(int entityId){
        return (entityId << 4 | 0x1);
    }

    public static int zmqID2EntityID(int zmqId){
        return zmqId >> 4;
    }

    public static void zmqSetSocketOpt(ZMQ.Socket socket, int option, int optVal){
        switch (option){
            case ZMQ_IDENTITY:
                socket.setIdentity(String.valueOf(optVal).getBytes());break;
            case ZMQ_ROUTER_MANDATORY:
                socket.setRouterMandatory(optVal != 0 ? true : false);break;
            case ZMQ_SNDBUF:
                socket.setSendBufferSize(optVal);break;
            case ZMQ_RCVBUF:
                socket.setReceiveBufferSize(optVal);break;
        }
    }

    public static void zmqBind(ZMQ.Socket socket, String connectAddr){
        socket.bind(connectAddr);
        //catch zma::error_t
    }

    public static void zmqConnectSend(ZMQ.Socket socket, String connectAddr, int zmqId,
                                 ByteBuffer msgBuf){
        socket.connect(connectAddr);
        boolean suc = false;
        ZMsg msg = new ZMsg();
        msg.push(msgBuf.array());
        msg.push("");
        msg.push(String.valueOf(zmqId));
        try {
            do {
                Thread.sleep(100);
                suc = msg.send(socket, false);
                if (suc == true) {
                    break;
                }
                try {
                    Thread.sleep(0, 500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } while (true);
        }catch(Exception e) {
            System.out.println(e.toString());
            System.out.println(msg.toString());
        }
    }

    // True for received, false for not
    public static ByteBuffer zmqRecvAsync(ZMQ.Socket socket){
        boolean received = false;
        ZMsg msg = ZMsg.recvMsg(socket, ZMQ.DONTWAIT);
        if(msg == null)
            return null;
        else {
            msg.pop();
            return ByteBuffer.wrap(msg.pop().getData());
        }
    }
    public static ByteBuffer zmqRecvAsync(ZMQ.Socket socket, IntBox zmqId){

        ZMsg msg = ZMsg.recvMsg(socket, ZMQ.DONTWAIT);
        if (msg == null)
            return null;
        else {
            zmqId.intValue = Integer.valueOf(msg.popString());
            msg.pop();
            return ByteBuffer.wrap(msg.pop().getData());
        }
    }

    public static ByteBuffer zmqRecv(ZMQ.Socket sock, IntBox zmqId) {
        ZMsg msg = ZMsg.recvMsg(sock);
        zmqId.intValue = Integer.valueOf(msg.popString());
        msg.pop();
        return ByteBuffer.wrap(msg.pop().getData());
    }

    /**
     * return number of bytes sent
     * @param sock
     * @param data
     * @param flag
     * @return
     */

    /**
     * 0 means cannot be sent, try again;
     0 should not happen unless flag = ZMQ_DONTWAIT
     * @param sock
     * @param zmqId
     * @param data
     * @return
     */
    public static boolean zmqSend(ZMQ.Socket sock, int zmqId, ByteBuffer data){

        ZMsg msg = new ZMsg();
        msg.push(data.array());
        msg.push("");
        msg.push(String.valueOf(zmqId));
        return msg.send(sock);

    }

}
