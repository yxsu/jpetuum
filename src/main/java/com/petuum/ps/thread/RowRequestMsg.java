package com.petuum.ps.thread;

import com.petuum.ps.common.NumberedMsg;
import zmq.Msg;

import java.nio.ByteBuffer;

/**
 * Created by suyuxin on 14-8-25.
 */
public class RowRequestMsg extends NumberedMsg {

    protected static final int TABLE_ID_OFFSET = NumberedMsg.getSize();
    protected static final int ROW_ID_OFFSET = TABLE_ID_OFFSET + 1 * INT_LENGTH;
    protected static final int START_OFFSET = TABLE_ID_OFFSET + 2 * INT_LENGTH;
    protected static final int OFFSET_OFFSET = TABLE_ID_OFFSET + 3 * INT_LENGTH;
    protected static final int CLOCK_OFFSET = ROW_ID_OFFSET + 4 * INT_LENGTH;
    public RowRequestMsg(ByteBuffer msgBuf) {
        super(msgBuf);
        if(msgBuf == null)
            sequence = ByteBuffer.allocate(getSize());
        sequence.putInt(MSG_TYPE_OFFSET, K_ROW_REQUEST);
    }

    public int getTableId() {
        return sequence.getInt(TABLE_ID_OFFSET);
    }

    public void setTableId(int id) {
        sequence.putInt(TABLE_ID_OFFSET, id);
    }

    public int getRowId() {
        return sequence.getInt(ROW_ID_OFFSET);
    }

    public int getStart() {
        return sequence.getInt(START_OFFSET);
    }

    public int getOffset() {
        return sequence.getInt(OFFSET_OFFSET);
    }

    public void setStart(int columnId) {
        sequence.putInt(START_OFFSET, columnId);
    }

    public void setOffset(int offset) {
        sequence.putInt(OFFSET_OFFSET, offset);
    }

    public void setRowId(int id) {
        sequence.putInt(ROW_ID_OFFSET, id);
    }
    public int getClock() {
        return sequence.getInt(CLOCK_OFFSET);
    }

    public void setClock(int clock) {
        sequence.putInt(CLOCK_OFFSET, clock);
    }

    public static int getSize() {
        return CLOCK_OFFSET + INT_LENGTH;
    }

}
