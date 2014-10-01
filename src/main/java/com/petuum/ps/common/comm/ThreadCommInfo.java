package com.petuum.ps.common.comm;

import org.zeromq.ZMQ;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * Created by ZengJichuan on 2014/8/23.
 */
public class ThreadCommInfo{
    public int entityId;
    ZMQ.Socket inprocSock;
    ZMQ.Socket interprocSock;
    ZMQ.PollItem inprocPollItem;
    ZMQ.PollItem interprocPollItem;
    ZMQ.Poller pollItems;

    public BlockingQueue<Map.Entry<Integer, ByteBuffer>> inprocQueue;
    public int lType;
    public int pollSize;

    public int numBytesInprocSendBuff;
    public int numBytesInprocRecvBuff;
    public int numBytesInterprocSendBuff;
    public int numBytesInterprocRecvBuff;

}