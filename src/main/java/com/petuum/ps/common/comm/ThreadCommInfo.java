package com.petuum.ps.common.comm;

import org.zeromq.ZMQ;

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

    public int lType;
    public int pollSize;

    public int numBytesInprocSendBuff;
    public int numBytesInprocRecvBuff;
    public int numBytesInterprocSendBuff;
    public int numBytesInterprocRecvBuff;

}