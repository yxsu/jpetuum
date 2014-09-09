package com.petuum.ps.thread;

import com.petuum.ps.common.NumberedMsg;
import zmq.Msg;

import java.nio.ByteBuffer;

/**
 * Created by suyuxin on 14-8-27.
 */
public class AppThreadDeregMsg extends NumberedMsg {
    public AppThreadDeregMsg(ByteBuffer msgBuf) {
        super(msgBuf);
        if(msgBuf == null)
            sequence = ByteBuffer.allocate(getSize());
        sequence.putInt(MSG_TYPE_OFFSET, K_APP_THREAD_DEREG);
    }

}
