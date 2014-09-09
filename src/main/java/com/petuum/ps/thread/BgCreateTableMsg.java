package com.petuum.ps.thread;

import com.petuum.ps.common.NumberedMsg;
import org.omg.CORBA.INITIALIZE;
import zmq.Msg;

import java.nio.ByteBuffer;

/**
 * Created by suyuxin on 14-8-25.
 */
public class BgCreateTableMsg extends NumberedMsg {

    protected static final int TABLE_ID_OFFSET = NumberedMsg.getSize();
    protected static final int STALENESS_OFFSET = TABLE_ID_OFFSET + 1 * INT_LENGTH;
    protected static final int ROW_TYPE_OFFSET = TABLE_ID_OFFSET + 2 * INT_LENGTH;
    protected static final int ROW_CAPACITY_OFFSET = TABLE_ID_OFFSET + 3 * INT_LENGTH;
    protected static final int PROCESS_CACHE_CAPACITY_OFFSET = TABLE_ID_OFFSET + 4 * INT_LENGTH;
    protected static final int THREAD_CACHE_CAPACITY_OFFSET = TABLE_ID_OFFSET + 5 * INT_LENGTH;
    protected static final int OPLOG_CAPACITY_OFFSET = TABLE_ID_OFFSET + 6 * INT_LENGTH;

    public BgCreateTableMsg(ByteBuffer msgBuf) {
        super(msgBuf);
        if(msgBuf == null)
            sequence = ByteBuffer.allocate(getSize());
        sequence.putInt(MSG_TYPE_OFFSET, K_BG_CREATE_TABLE);
    }

    public static int getSize() {
        return OPLOG_CAPACITY_OFFSET + INT_LENGTH;
    }

    public int getTableId() {
        return sequence.getInt(TABLE_ID_OFFSET);
    }

    public int getStaleness() {
        return sequence.getInt(STALENESS_OFFSET);
    }

    public int getRowType() {
        return sequence.getInt(ROW_TYPE_OFFSET);
    }

    public int getRowCapacity() {
        return sequence.getInt(ROW_CAPACITY_OFFSET);
    }

    public int getProcessCacheCapacity() {
        return sequence.getInt(PROCESS_CACHE_CAPACITY_OFFSET);
    }

    public int getThreadCacheCapacity() {
        return sequence.getInt(THREAD_CACHE_CAPACITY_OFFSET);
    }

    public int getOplogCapacity() {
        return sequence.getInt(OPLOG_CAPACITY_OFFSET);
    }

    public void setTableId(int id) {
        sequence.putInt(TABLE_ID_OFFSET, id);
    }

    public void setStaleness(int staleness) {
        sequence.putInt(STALENESS_OFFSET, staleness);
    }

    public void setRowType(int rowType) {
        sequence.putInt(ROW_TYPE_OFFSET, rowType);
    }

    public void setRowCapacity(int rowCapacity) {
        sequence.putInt(ROW_CAPACITY_OFFSET, rowCapacity);
    }

    public void setProcessCacheCapacity(int processCacheCapacity) {
        sequence.putInt(PROCESS_CACHE_CAPACITY_OFFSET, processCacheCapacity);
    }

    public void setThreadCacheCapacity(int threadCacheCapacity) {
        sequence.putInt(THREAD_CACHE_CAPACITY_OFFSET, threadCacheCapacity);
    }

    public void setOplogCapacity(int oplogCapacity) {
        sequence.putInt(OPLOG_CAPACITY_OFFSET, oplogCapacity);
    }

}
