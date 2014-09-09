package com.petuum.ps.thread;

import zmq.Msg;

import java.nio.ByteBuffer;

/**
 * Created by suyuxin on 14-8-27.
 */
public class ServerPushRowMsg extends ArbitrarySizedMsg {

    protected static final int CLOCK_OFFSET = ArbitrarySizedMsg.getHeaderSize();
    protected static final int VERSION_OFFSET = CLOCK_OFFSET + INT_LENGTH;
    protected static final int IS_CLOCK_OFFSET = CLOCK_OFFSET + 2 * INT_LENGTH;
    public ServerPushRowMsg(ByteBuffer msgBuf) {
        super(msgBuf);
        sequence.putInt(MSG_TYPE_OFFSET, K_SERVER_PUSH_ROW);
    }

    public ServerPushRowMsg(int avaiSize) {
        super(null);
        sequence = ByteBuffer.allocate(getHeaderSize() + avaiSize);
        sequence.putInt(MSG_TYPE_OFFSET, K_SERVER_PUSH_ROW);
        sequence.putInt(AVAI_SIZE_OFFSET, avaiSize);
    }

    public void setAvaiSize(int avaiSize) {
        int version = getVersion();
        boolean isClock = getIsClock();
        int clock = getClock();
        sequence = ByteBuffer.allocate(getHeaderSize() + avaiSize);
        sequence.putInt(MSG_TYPE_OFFSET, K_SERVER_PUSH_ROW);
        sequence.putInt(AVAI_SIZE_OFFSET, avaiSize);
        setIsClock(isClock);
        setClock(clock);
        setVersion(version);
    }

    public int getVersion() {
        return sequence.getInt(VERSION_OFFSET);
    }

    public void setVersion(int version) {
        sequence.putInt(VERSION_OFFSET, version);
    }

    public boolean getIsClock() {
        return sequence.get(IS_CLOCK_OFFSET) == 1;
    }

    public int getClock() {
        return sequence.getInt(CLOCK_OFFSET);
    }

    public void setClock(int clock) {
        sequence.putInt(CLOCK_OFFSET, clock);
    }
    public void setIsClock(boolean isClock) {
        Byte temp = 0;
        if(isClock) {
            temp = 1;
        }
        sequence.put(IS_CLOCK_OFFSET, temp);
    }

    public static int getHeaderSize() {
        return IS_CLOCK_OFFSET + 1;//IS_CLOCK_OFFSET is boolean type
    }

    public ByteBuffer getData() {
        byte[] byteList = sequence.array();
        return ByteBuffer.wrap(byteList, getHeaderSize(), byteList.length - getHeaderSize());
    }
}
