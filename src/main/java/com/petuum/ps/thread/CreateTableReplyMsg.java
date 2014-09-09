package com.petuum.ps.thread;

import com.petuum.ps.common.NumberedMsg;
import zmq.Msg;

import java.nio.ByteBuffer;

/**
 * Created by suyuxin on 14-8-25.
 */
public class CreateTableReplyMsg extends NumberedMsg {

    protected static final int TABLE_ID_OFFSET = NumberedMsg.getSize();
    public CreateTableReplyMsg(ByteBuffer msgBuf) {
        super(msgBuf);
        if(msgBuf == null)
            sequence = ByteBuffer.allocate(getSize());
        sequence.putInt(MSG_TYPE_OFFSET, K_CREATE_TABLE_REPLY);
    }

    public int getTableId() {
        return sequence.getInt(TABLE_ID_OFFSET);
    }

    public void setTableId(int id) {
        sequence.putInt(TABLE_ID_OFFSET, id);
    }

    public static int getSize() {
        return TABLE_ID_OFFSET + INT_LENGTH;
    }
}
