package com.petuum.ps.thread;

import com.petuum.ps.common.NumberedMsg;
import zmq.Msg;

import java.nio.ByteBuffer;

/**
 * Created by suyuxin on 14-8-25.
 */
public class CreateTableMsg extends NumberedMsg {

    protected static final int TABLE_ID_OFFSET = NumberedMsg.getSize();
    protected static final int STALENESS_OFFSET = TABLE_ID_OFFSET + 1 * INT_LENGTH;
    protected static final int ROW_TYPE_OFFSET = TABLE_ID_OFFSET + 2 * INT_LENGTH;
    protected static final int ROW_CAPACITY_OFFSET = TABLE_ID_OFFSET + 3 * INT_LENGTH;

    public CreateTableMsg(ByteBuffer msgBuf) {
        super(msgBuf);
        if(msgBuf == null)
            sequence = ByteBuffer.allocate(getSize());
        sequence.putInt(MSG_TYPE_OFFSET, K_CREATE_TABLE);
    }

    public static int getSize() {
        return ROW_CAPACITY_OFFSET + INT_LENGTH;
    }

    public int getTableId() {
        return sequence.getInt(TABLE_ID_OFFSET);
    }

    public void setTableId(int id) {
        sequence.putInt(TABLE_ID_OFFSET, id);
    }

    public int getStaleness() {
        return sequence.getInt(STALENESS_OFFSET);
    }

    public void setStaleness(int staleness) {
        sequence.putInt(STALENESS_OFFSET, staleness);
    }

    public int getRowType() {
        return sequence.getInt(ROW_TYPE_OFFSET);
    }

    public void setRowType(int rowType) {
        sequence.putInt(ROW_TYPE_OFFSET, rowType);
    }
    public int getRowCapacity() {
        return sequence.getInt(ROW_CAPACITY_OFFSET);
    }

    public void setRowCapacity(int rowCapacity) {
        sequence.putInt(ROW_CAPACITY_OFFSET, rowCapacity);
    }
}
