package com.petuum.ps.common.comm;

import com.google.common.base.Preconditions;
import com.petuum.ps.common.util.IntBox;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by admin on 2014/8/11.
 */
public class CommBus {
    public static final int K_NONE=0;
    public static final int K_IN_PROC=1;
    public static final int K_INTER_PROC=2;

    private static String kInProcPrefix ="ipc://comm_bus";
    private static String kInterProcPrefix="tcp://";

    private ZContext zmqContext;
    private int eStart;
    private int eEnd;
    private ThreadLocal<ThreadCommInfo> threadInfo = new ThreadLocal<ThreadCommInfo>();


    public void close() {
        zmqContext.destroy();
    }

    public static class Config{
        /**
         *  My thread id.
         */
        public int entityId;
        /**
         * What should I listen to?
         */
        public int lType;

        /**
         *  In the format of "ip:port", such as "192.168.1.1:9999". It must be set
         *  if ((ltype_ & kInterProc) == true)
         */
        public String networkAddr;

        public int numBytesInprocSendBuff;
        public int numBytesInprocRecvBuff;
        public int numBytesInterprocSendBuff;
        public int numBytesInterprocRecvBuff;

        public Config() {
            this.entityId = 0;
            this.lType = CommBus.K_NONE;
            this.numBytesInprocSendBuff = 0;
            this.numBytesInprocRecvBuff = 0;
            this.numBytesInterprocSendBuff = 0;
            this.numBytesInterprocRecvBuff = 0;
        }

        public Config(int entityId, int lType, String networkAddr) {
            this.entityId = entityId;
            this.lType = lType;
            this.networkAddr = networkAddr;
            this.numBytesInprocSendBuff = 0;
            this.numBytesInprocRecvBuff = 0;
            this.numBytesInterprocSendBuff = 0;
            this.numBytesInterprocRecvBuff = 0;
        }
    }

    private static void makeInprocAddr(int entityId, StringBuffer result){
        result.setLength(0);
        result.append(kInProcPrefix);
        result.append(":");
        result.append(Integer.toString(entityId));
    }

    private static void makeInterprocAddr(String networkAddr, StringBuffer result){
        result.setLength(0);
        result.append(kInterProcPrefix);
        result.append(networkAddr);
    }

    /**
     * Config socket options
     * @param socket
     * @param id
     * @param numBytesSendBuff
     * @param numBytesRecvBuff
     */
    private static void setUpRouterSocket(ZMQ.Socket socket, int id,
                                          int numBytesSendBuff, int numBytesRecvBuff){
        int myId = ZmqUtil.entityID2ZmqID(id);
        ZmqUtil.zmqSetSocketOpt(socket, ZmqUtil.ZMQ_IDENTITY, myId);

        ZmqUtil.zmqSetSocketOpt(socket, ZmqUtil.ZMQ_ROUTER_MANDATORY, 0);

        if (numBytesSendBuff != 0){
            ZmqUtil.zmqSetSocketOpt(socket, ZmqUtil.ZMQ_SNDBUF, numBytesSendBuff);
        }

        if (numBytesRecvBuff != 0){
            ZmqUtil.zmqSetSocketOpt(socket, ZmqUtil.ZMQ_RCVBUF, numBytesRecvBuff);
        }
    }

    /**
     * Register a thread, set up necessary commnication channel.
     * For network communication (TCP), zmq::bind may be called after
     * zmq::connect. But for inproc communication, zmq::bind has to be called
     * before zmq::connect.
     * For CommBus, a thread must have successfully registered before
     * other thread may connect to it.
     * @param config
     */
    public void threadRegister(Config config){
        Preconditions.checkArgument(threadInfo.get() == null);
        threadInfo.set(new ThreadCommInfo());
        threadInfo.get().entityId = config.entityId;
        threadInfo.get().lType = config.lType;

        threadInfo.get().numBytesInprocSendBuff = config.numBytesInprocSendBuff;
        threadInfo.get().numBytesInprocRecvBuff = config.numBytesInprocRecvBuff;
        threadInfo.get().numBytesInterprocSendBuff = config.numBytesInterprocSendBuff;
        threadInfo.get().numBytesInterprocRecvBuff = config.numBytesInterprocRecvBuff;

        if((config.lType & K_IN_PROC) != 0){
            threadInfo.get().inprocQueue = new SynchronousQueue<Map.Entry<Integer, ByteBuffer>>(true);

            threadInfo.get().inprocSock = zmqContext.createSocket(ZMQ.ROUTER);
            ZMQ.Socket sock = threadInfo.get().inprocSock;
            setUpRouterSocket(sock, config.entityId,
                    config.numBytesInprocSendBuff, config.numBytesInprocRecvBuff);
            StringBuffer bindAddr = new StringBuffer();
            makeInprocAddr(config.entityId, bindAddr);
            ZmqUtil.zmqBind(sock, bindAddr.toString());
        }

        if((config.lType & K_INTER_PROC) != 0){
            threadInfo.get().interprocSock = zmqContext.createSocket(ZMQ.ROUTER);
            ZMQ.Socket sock = threadInfo.get().interprocSock;
            setUpRouterSocket(sock, config.entityId,
                    config.numBytesInprocSendBuff, config.numBytesInprocRecvBuff);
                    //???why not use inter
            StringBuffer bindAddr = new StringBuffer();
            makeInterprocAddr(config.networkAddr, bindAddr);
            ZmqUtil.zmqBind(sock, bindAddr.toString());
        }
    }

    public void threadDeregister(){
        threadInfo.remove();
    }

    /**
     * Connect to a local thread
     * Info is a customer-defined number to be included in the Connect message,
     * how to use it is up to the customer.
     * @param entityId
     * @param connectMsg
     */
    public void connectTo(int entityId, ByteBuffer connectMsg){
        Preconditions.checkArgument(isLocalEntity(entityId));
        ZMQ.Socket sock = threadInfo.get().inprocSock;

        if (sock == null){
            threadInfo.get().inprocSock = zmqContext.createSocket(ZMQ.ROUTER);
            sock = threadInfo.get().inprocSock;
            setUpRouterSocket(sock, threadInfo.get().entityId,
                    threadInfo.get().numBytesInprocSendBuff,
                    threadInfo.get().numBytesInprocRecvBuff);
        }

        StringBuffer connectAddr = new StringBuffer();
        makeInprocAddr(entityId, connectAddr);
        int zmqId = ZmqUtil.entityID2ZmqID(entityId);
        ZmqUtil.zmqConnectSend(sock, connectAddr.toString(), zmqId, connectMsg);
    }

    /**
     * Connect to a remote thread.
     * @param entityId
     * @param networkAddr
     * @param connectMsg
     */
    public void connectTo(int entityId, String networkAddr, ByteBuffer connectMsg){
        Preconditions.checkArgument(!isLocalEntity(entityId));
        ZMQ.Socket sock = threadInfo.get().interprocSock;
        if (sock == null){
            threadInfo.get().interprocSock = zmqContext.createSocket(ZMQ.ROUTER);
            sock = threadInfo.get().interprocSock;
            setUpRouterSocket(sock, threadInfo.get().entityId,
                    threadInfo.get().numBytesInterprocSendBuff,
                    threadInfo.get().numBytesInterprocRecvBuff);
        }
        StringBuffer connectAddr = new StringBuffer();
        makeInterprocAddr(networkAddr, connectAddr);
        int zmqId = ZmqUtil.entityID2ZmqID(entityId);
        ZmqUtil.zmqConnectSend(sock, connectAddr.toString(), zmqId, connectMsg);
    }

    public boolean send(int entityId, ByteBuffer data){
        ZMQ.Socket sock;
        if(isLocalEntity(entityId))
            sock = threadInfo.get().inprocSock;
        else
            sock = threadInfo.get().interprocSock;
        int recvId = ZmqUtil.entityID2ZmqID(entityId);
        return ZmqUtil.zmqSend(sock, recvId, data);         //is necessary to return size?
    }

    public boolean sendInproc(int entityId, ByteBuffer data){
        ZMQ.Socket sock = threadInfo.get().inprocSock;
        int recvId = ZmqUtil.entityID2ZmqID(entityId);
        return ZmqUtil.zmqSend(sock, recvId, data);
    }

    public boolean sendInterproc(int entityId, ByteBuffer data){
        ZMQ.Socket sock = threadInfo.get().interprocSock;
        int recvId = ZmqUtil.entityID2ZmqID(entityId);
        return ZmqUtil.zmqSend(sock, recvId, data);
    }

    public ByteBuffer recv(IntBox entityId){
        if(threadInfo.get().pollItems == null) {
            threadInfo.get().pollItems = new ZMQ.Poller(2);
            threadInfo.get().pollItems.register(threadInfo.get().inprocSock, ZMQ.Poller.POLLIN);
            threadInfo.get().pollItems.register(threadInfo.get().interprocSock, ZMQ.Poller.POLLIN);
        }
        threadInfo.get().pollItems.poll();
        ZMQ.Socket sock;
        if (threadInfo.get().pollItems.pollin(0)){
            sock = threadInfo.get().inprocSock;
        }else {
            sock = threadInfo.get().interprocSock;
        }
        IntBox senderId = new IntBox();
        ByteBuffer buffer = ZmqUtil.zmqRecv(sock, senderId);
        entityId.intValue = ZmqUtil.zmqID2EntityID(senderId.intValue);
        return buffer;
    }

    public ByteBuffer recvAsync(IntBox entityId){
        if(threadInfo.get().pollItems == null) {
            threadInfo.get().pollItems = new ZMQ.Poller(2);
            threadInfo.get().pollItems.register(threadInfo.get().inprocSock, ZMQ.Poller.POLLIN);
            threadInfo.get().pollItems.register(threadInfo.get().interprocSock, ZMQ.Poller.POLLIN);
        }
        threadInfo.get().pollItems.poll();
        ZMQ.Socket sock;
        if (threadInfo.get().pollItems.pollin(0)){
            sock = threadInfo.get().inprocSock;
        }else if (threadInfo.get().pollItems.pollin(1)){
            sock = threadInfo.get().interprocSock;
        }else
            return null;
        IntBox senderId = new IntBox();
        ByteBuffer buffer = ZmqUtil.zmqRecv(sock, senderId);
        entityId.intValue = ZmqUtil.zmqID2EntityID(senderId.intValue);
        return buffer;
    }

    public ByteBuffer recvTimeOut(IntBox entityId, long timeoutMilli){
        if(threadInfo.get().pollItems == null) {
            threadInfo.get().pollItems = new ZMQ.Poller(2);
            threadInfo.get().pollItems.register(threadInfo.get().inprocSock, ZMQ.Poller.POLLIN);
            threadInfo.get().pollItems.register(threadInfo.get().interprocSock, ZMQ.Poller.POLLIN);
        }
        threadInfo.get().pollItems.poll(timeoutMilli);
        ZMQ.Socket sock;
        if (threadInfo.get().pollItems.pollin(0)){
            sock = threadInfo.get().inprocSock;
        }else if (threadInfo.get().pollItems.pollin(1)){
            sock = threadInfo.get().interprocSock;
        }else
            return null;
        IntBox senderId = new IntBox();
        ByteBuffer buffer = ZmqUtil.zmqRecv(sock, senderId);
        entityId.intValue = ZmqUtil.zmqID2EntityID(senderId.intValue);
        return buffer;
    }
    public ByteBuffer recvInproc(IntBox entityId){
        IntBox senderId = new IntBox();
        ByteBuffer buffer = ZmqUtil.zmqRecv(threadInfo.get().inprocSock, senderId);
        entityId.intValue = ZmqUtil.zmqID2EntityID(senderId.intValue);
        return buffer;
    }
    public ByteBuffer recvInprocAsync(IntBox entityId){
        IntBox senderId = new IntBox();
        ByteBuffer buffer = ZmqUtil.zmqRecvAsync(threadInfo.get().inprocSock, senderId);
        if (buffer != null){
            entityId.intValue = ZmqUtil.zmqID2EntityID(senderId.intValue);
        }
        return buffer;
    }
    public ByteBuffer recvInprocTimeout(IntBox entityId, long timeoutMilli){
        if(threadInfo.get().inprocPollItem == null) {
            threadInfo.get().inprocPollItem =
                    new ZMQ.PollItem(threadInfo.get().inprocSock, ZMQ.Poller.POLLIN);
        }
        ZMQ.Poller poller = new ZMQ.Poller(1);
        poller.register(threadInfo.get().inprocPollItem);
        poller.poll(timeoutMilli);
        ZMQ.Socket sock;
        if (poller.pollin(0)){
            sock = threadInfo.get().inprocSock;
        }else{
            return null;
        }
        IntBox senderId = new IntBox();
        ByteBuffer buffer = ZmqUtil.zmqRecv(sock, senderId);
        entityId.intValue = ZmqUtil.zmqID2EntityID(senderId.intValue);
        return buffer;
    }

    public ByteBuffer recvInterproc(IntBox entityId){
        IntBox senderId = new IntBox();
        ByteBuffer buffer = ZmqUtil.zmqRecv(threadInfo.get().interprocSock, senderId);
        entityId.intValue = ZmqUtil.zmqID2EntityID(senderId.intValue);
        return buffer;
    }

    public ByteBuffer recvInterprocAsync(IntBox entityId){
        IntBox senderId = new IntBox();
        ByteBuffer buffer = ZmqUtil.zmqRecvAsync(threadInfo.get().interprocSock, senderId);
        if (buffer !=  null){
            entityId.intValue = ZmqUtil.zmqID2EntityID(senderId.intValue);
        }
        return buffer;
    }
    public ByteBuffer recvInterprocTimeout(IntBox entityId, long timeoutMilli){
        if(threadInfo.get().interprocPollItem == null) {
            threadInfo.get().interprocPollItem =
                    new ZMQ.PollItem(threadInfo.get().interprocSock, ZMQ.Poller.POLLIN);
        }
        ZMQ.Poller poller = new ZMQ.Poller(1);
        poller.register(threadInfo.get().interprocPollItem);
        poller.poll(timeoutMilli);
        ZMQ.Socket sock;
        if (poller.pollin(0)){
            sock = threadInfo.get().interprocSock;
        }else{
            return null;
        }
        IntBox senderId = new IntBox();
        ByteBuffer buffer = ZmqUtil.zmqRecv(sock, senderId);
        entityId.intValue = ZmqUtil.zmqID2EntityID(senderId.intValue);

        return buffer;
    }

    public Method recvFunc;
    public Method recvTimeOutFunc;
    public Method recvAsyncFunc;
    public Method recvWrapperFunc;
    public Method sendFunc;

    public boolean isLocalEntity(int entityId){
        return (eStart <= entityId) && (entityId <= eEnd);
    }

    public CommBus(int eStart, int eEnd, int numZmqThreads) {
        this.eStart = eStart;
        this.eEnd = eEnd;
        zmqContext = new ZContext(numZmqThreads);
        //originally it will throw zmq:error, but seems not in java
    }

}

