package com.petuum.ps.common.test;

import com.petuum.ps.common.NumberedMsg;
import com.petuum.ps.common.comm.CommBus;
import com.petuum.ps.common.comm.ZmqUtil;
import com.petuum.ps.common.util.IntBox;
import com.petuum.ps.thread.ClientConnectMsg;
import com.petuum.ps.thread.ServerConnectMsg;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import zmq.Msg;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.*;

/**
 * Created by Yuxin Su on 2014/8/21.
 */
public class CommBusTest {
    private static ZContext zmqContext = new ZContext(1);
    private static CommBus commBus = new CommBus(0, 100, 1);
    private static Logger log = LogManager.getLogger(CommBusTest.class);

    public static Thread client0 = new Thread(new Runnable() {
        public void run() {
            //ZMQ.Socket socket = zmqContext.createSocket(ZMQ.ROUTER);
            //int entityID = 0;//ZmqUtil.entityID2ZmqID(0);
            //socket.setIdentity(String.valueOf(entityID).getBytes());
            //socket.bind("ipc://comm_bus:5");
            CommBus.Config config = new CommBus.Config(0, CommBus.K_IN_PROC, "");
            commBus.threadRegister(config);
            log.info("Client 0 is established");
            IntBox senderId = new IntBox();
            while(true) {
                //ByteBuffer buffer = ZmqUtil.zmqRecv(socket, senderId);
                ByteBuffer buffer = commBus.recvInproc(senderId);
                NumberedMsg msg = new NumberedMsg(buffer);
                if(msg.getMsgType() == NumberedMsg.K_CLIENT_CONNECT){
                    log.info("Client Connect Msg is received");
                } else if(msg.getMsgType() == NumberedMsg.K_SERVER_CONNECT) {
                    log.info("Server Connect Msg is received");
                }
            }
        }
    });

    public static Thread client1 = new Thread(new Runnable() {
        public void run() {
            //ZMQ.Socket socket = zmqContext.createSocket(ZMQ.ROUTER);
            //int entityID = 1;//ZmqUtil.entityID2ZmqID(1);
            //socket.setIdentity(String.valueOf(entityID).getBytes());
            //socket.bind("ipc://comm_bus:6");
            CommBus.Config config = new CommBus.Config(8, CommBus.K_IN_PROC, "");
            commBus.threadRegister(config);
            log.info("Client 1 is established");
            ClientConnectMsg msg = new ClientConnectMsg(null);
            for(int i = 0; i < 10; i++) {
                //while(true) {
                //ZmqUtil.zmqConnectSend(socket, "ipc://comm_bus:5", 0, msg.getByteBuffer());
                System.out.println("Sending ... " + String.valueOf(i));
                msg.setClientId(i);
                commBus.connectTo(0, msg.getByteBuffer());
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                //    }
            }
        }
    });

    public static Thread client2 = new Thread(new Runnable() {
        public void run() {
            CommBus.Config config = new CommBus.Config(9, CommBus.K_IN_PROC, "");
            commBus.threadRegister(config);
            log.info("Client 2 is established");
            ServerConnectMsg msg = new ServerConnectMsg(null);
            while(true) {
                //ZmqUtil.zmqConnectSend(socket, "ipc://comm_bus:5", 0, msg.getByteBuffer());
                commBus.connectTo(7, msg.getByteBuffer());
                try {
                    Thread.sleep(6000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    });

    public static void main(String[] args) throws InterruptedException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        //client0.start();
        client1.start();
        //client2.start();
        //client0.join();
        client1.join();
        //client2.join();
    }
}