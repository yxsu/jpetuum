package com.petuum.ps.thread;

import com.petuum.ps.common.Row;
import zmq.Msg;

import javax.crypto.Mac;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * Created by suyuxin on 14-8-27.
 */
public class ServerRowRequestReplyMsg extends ArbitrarySizedMsg {

    protected static final int TABLE_ID_OFFSET = ArbitrarySizedMsg.getHeaderSize();
    protected static final int ROW_ID_OFFSET = TABLE_ID_OFFSET + 1 * INT_LENGTH;
    protected static final int CLOCK_OFFSET = TABLE_ID_OFFSET + 2 * INT_LENGTH;
    protected static final int VERSION_OFFSET = TABLE_ID_OFFSET + 3 * INT_LENGTH;
    protected static final int ROW_SIZE_OFFSET = TABLE_ID_OFFSET + 4 * INT_LENGTH;

//    public ServerRowRequestReplyMsg(Msg msg) {
//        super(msg);
//        if(msg == null)
//            sequence = ByteBuffer.allocate(ArbitrarySizedMsg.getSize());
//        sequence.putInt(MSG_TYPE_OFFSET, K_SERVER_ROW_REQUEST_REPLY);
//    }

    public ServerRowRequestReplyMsg(ByteBuffer msgBuf) {
        super(msgBuf);
        if (msgBuf == null)
            sequence = ByteBuffer.allocate(getSize());
        sequence.putInt(MSG_TYPE_OFFSET, K_SERVER_ROW_REQUEST_REPLY);
    }

    public static int getSize() {
        return ROW_SIZE_OFFSET + INT_LENGTH;
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

    public void setRowId(int id) {
        sequence.putInt(ROW_ID_OFFSET, id);
    }

    public int getClock() {
        return sequence.getInt(CLOCK_OFFSET);
    }

    public void setClock(int clock) {
        sequence.putInt(CLOCK_OFFSET, clock);
    }

    public int getVersion() {
        return sequence.getInt(VERSION_OFFSET);
    }

    public void setVersion(int version) {
        sequence.putInt(VERSION_OFFSET, version);
    }

    public int getRowSize() {
        return sequence.getInt(ROW_SIZE_OFFSET);
    }

    public void setRowSize(int rowSize) {
        sequence.putInt(ROW_SIZE_OFFSET, rowSize);
    }

    public static int getHeaderSize() {
        return ROW_SIZE_OFFSET + INT_LENGTH;
    }

    public void setRowData(ByteBuffer buffer){
        assert buffer != null;
        sequence = ByteBuffer.allocate(getHeaderSize() + buffer.capacity());
        sequence.putInt(MSG_TYPE_OFFSET, K_SERVER_ROW_REQUEST_REPLY);
        sequence.position(getHeaderSize());
        sequence.put(buffer);
    }

    public ByteBuffer getRowData() {
        byte [] bytes = Arrays.copyOfRange(sequence.array(), getHeaderSize(), sequence.capacity());
        return ByteBuffer.wrap(bytes);
    }
}
