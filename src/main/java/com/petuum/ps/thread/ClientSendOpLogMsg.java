package com.petuum.ps.thread;

import com.petuum.ps.common.NumberedMsg;
import zmq.Msg;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Created by suyuxin on 14-8-27.
 */
public class ClientSendOpLogMsg extends ArbitrarySizedMsg {

    protected static final int IS_CLOCK_OFFSET = ArbitrarySizedMsg.getHeaderSize();
    protected static final int CLIENT_ID_OFFSET = IS_CLOCK_OFFSET + 1;//IS_CLOCK_OFFSET is boolean type
    protected static final int VERSION_OFFSET = CLIENT_ID_OFFSET + INT_LENGTH;

    public ClientSendOpLogMsg(ByteBuffer msgBuf) {
        super(msgBuf);
        if(msgBuf == null)
            sequence = ByteBuffer.allocate(ArbitrarySizedMsg.getSize());
        sequence.putInt(MSG_TYPE_OFFSET, K_CLIENT_SEND_OP_LOG);
    }

    public ClientSendOpLogMsg(int avaiSize) {
        super(null);
        sequence = ByteBuffer.allocate(getHeaderSize() + avaiSize);
        sequence.putInt(MSG_TYPE_OFFSET, K_CLIENT_SEND_OP_LOG);
        sequence.putInt(AVAI_SIZE_OFFSET, avaiSize);
    }

    public boolean getIsClock() {
        return sequence.get(IS_CLOCK_OFFSET) == 1;
    }

    public void setIsClock(boolean isClock) {
        Byte temp = 0;
        if(isClock) {
            temp = 1;
        }
        sequence.put(IS_CLOCK_OFFSET, temp);
    }

    public int getClientId() {
        return sequence.getInt(CLIENT_ID_OFFSET);
    }

    public void setClientId(int id) {
        sequence.putInt(CLIENT_ID_OFFSET, id);
    }

    public int getVersion() {
        return sequence.getInt(VERSION_OFFSET);
    }

    public void setVersion(int version) {
        sequence.putInt(VERSION_OFFSET, version);
    }

    public static int getHeaderSize() {
        return VERSION_OFFSET + INT_LENGTH;
    }
    public ByteBuffer getData() {
        //byte[] byteList = sequence.array();
        //return B(byteList, getHeaderSize(), byteList.length - getHeaderSize());
        sequence.position(getHeaderSize());
        return sequence.slice();
    }
}
