package com.petuum.ps.common;

import zmq.Msg;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;

/**
 * Created by suyuxin on 14-8-24.
 */
public class NumberedMsg {
    public static final int K_CLIENT_CONNECT = 0;
    public static final int K_SERVER_CONNECT = 1;
    public static final int K_APP_CONNECT = 2;
    public static final int K_BG_CREATE_TABLE = 3;
    public static final int K_CREATE_TABLE = 4;
    public static final int K_CREATE_TABLE_REPLY = 5;
    public static final int K_CREATED_ALL_TABLES = 6;
    public static final int K_ROW_REQUEST = 7;
    public static final int K_ROW_REQUEST_REPLY = 8;
    public static final int K_SERVER_ROW_REQUEST_REPLY = 9;
    public static final int K_BG_CLOCK = 10;
    public static final int K_BG_SEND_OP_LOG = 11;
    public static final int K_CLIENT_SEND_OP_LOG = 12;
    public static final int K_CONNECT_SERVER = 13;
    public static final int K_CLIENT_START = 14;
    public static final int K_APP_THREAD_DEREG = 15;
    public static final int K_CLIENT_SHUT_DOWN = 16;
    public static final int K_SERVER_SHUT_DOWN_ACK = 17;
    public static final int K_SERVER_PUSH_ROW = 18;
    public static final int K_MEM_TRANSFER = 50;

    protected static final int INT_LENGTH = 4;

    protected static final int MSG_TYPE_OFFSET = 0;
    protected static final int SEQ_NUM_OFFSET = 1 * INT_LENGTH;
    protected static final int ACK_NUM_OFFSET = 2 * INT_LENGTH;


    public static int getSize() {
        return ACK_NUM_OFFSET + INT_LENGTH;
    }

    public NumberedMsg(ByteBuffer buff) {
        if(buff != null)
            sequence = buff;
    }

    public int getMsgType() {
        return sequence.getInt(MSG_TYPE_OFFSET);
    }

    public int getSeqNum() {
        return sequence.getInt(SEQ_NUM_OFFSET);
    }

    public int getAckNum() {
        return sequence.getInt(ACK_NUM_OFFSET);
    }

    public ByteBuffer getByteBuffer() {
        return sequence;
    }

    protected ByteBuffer sequence;
}

