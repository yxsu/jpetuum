package com.petuum.ps.common.util;

import com.google.common.io.ByteStreams;
import com.google.common.primitives.Bytes;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * A buffer that allows appending records to it. Here we use java.nio.ByteBuffer.
 * Created by ZengJichuan on 2014/8/6.
 */
public class RecordBuff {
    private ByteBuffer mem;
    private int offset;
    private int memSize;
    private static Logger log = LogManager.getLogger(RecordBuff.class);

    public RecordBuff(ByteBuffer mem) {
        this.mem = mem;
        this.offset = 0;
        this.memSize = mem.capacity();
    }

    public ByteBuffer resetMem(ByteBuffer mem){
        this.mem = mem;
        offset = 0;
        this.memSize = mem.capacity();
        return mem;
    }

    public void resetOffset(){
        offset = 0;
    }

    public boolean append(int recordId, Object record, int recordSize) {
        if(offset + recordSize +Integer.BYTES +Integer.BYTES> memSize){
            return false;
        }
        mem.putInt(offset, recordId);
        offset += Integer.BYTES;
        mem.putInt(offset, recordSize);
        offset += Integer.BYTES;
        mem.put(SerializationUtils.serialize((java.io.Serializable) record), offset, recordSize);
        offset += recordSize;
        return true;
    }

    public int getMemUsedSize(){
        return offset;
    }

    public int getMemPos(){
        if (offset + Integer.BYTES > memSize){
            log.info("Exceeded! getMemPos() offset = "+ offset + " memSize = "+memSize);
            return -1;
        }
        int retPos = offset;
        offset += Integer.BYTES;
        return retPos;
    }

    public void putTableId(int tableIdPos, int tableId) {
        mem.putInt(tableIdPos, tableId);
    }
}
