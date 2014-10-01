package com.petuum.ps.thread;

import com.petuum.ps.common.NumberedMsg;

import java.nio.ByteBuffer;

/**
 * Created by suyuxin on 14-8-27.
 */
public class ConnectServerMsg extends NumberedMsg {
    public ConnectServerMsg(ByteBuffer msgBuf) {
        super(msgBuf);
        if(msgBuf == null)
            sequence = ByteBuffer.allocate(getSize());
        sequence.putInt(MSG_TYPE_OFFSET, K_CONNECT_SERVER);
    }
}
