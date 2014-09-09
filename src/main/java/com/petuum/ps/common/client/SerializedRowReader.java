package com.petuum.ps.common.client;

import com.petuum.ps.common.util.IntBox;
import com.petuum.ps.thread.GlobalContext;

import java.nio.ByteBuffer;

/**
 *  Provide sequential access to a byte string that's serialized rows.
 * Used to facilicate server reading row data.

 * st_separator : serialized_table_separator
 * st_end : serialized_table_end

 * Tables are serialized as the following memory layout
 * 1. int32_t : table id, could be st_separator or st_end
 * 2. int32_t : row id, could be st_separator or st_end
 * 3. size_t : serialized row size
 * 4. row data
 * repeat 1, 2, 3, 4
 * st_separator can not happen right after st_separator
 * st_end can not happen right after st_separator

 * Rules for serialization:
 * The serialized row data is guaranteed to end when seeing a st_end or with
 * finish reading the entire memory buffer.
 * When seeing a st_separator, there could be another table or no table
 * following. The latter happens only when the buffer reaches its memory
 * boundary.
 * Created by ZengJichuan on 2014/8/28.
 */
public class SerializedRowReader {
    private ByteBuffer mem;
    private int memSize;
    private int offset;
    private int currentTableId;

    public SerializedRowReader(ByteBuffer mem) {
        this.mem = mem;
        this.memSize = mem.capacity();
    }

    public boolean restart(){
        offset = 0;
        currentTableId = mem.getInt(offset);
        offset += Integer.BYTES;
        if(currentTableId == GlobalContext.getSerializedTableEnd()){
            return false;
        }
        return true;
    }

    /**
     * When starting, there are 4 possiblilities:
     * 1. finished reading the mem buffer
     * 2. encounter the end of an table but there are other tables following
     * (st_separator)
     * 3. encounter the end of an table but there is no other table following
     * (st_end)
     * 4. normal row data
     * @param tableId
     * @param rowId
     * @return
     */
    public ByteBuffer next(IntBox tableId, IntBox rowId){
        if(offset + Integer.BYTES > memSize)
            return null;
        rowId.intValue = mem.getInt(offset);
        offset += Integer.BYTES;
        do{
            if(rowId.intValue == GlobalContext.getSerializedTableSeparator()){
                if(offset + Integer.BYTES > memSize)
                    return null;
                currentTableId = mem.getInt(offset);
                if(offset + Integer.BYTES > memSize)
                    return null;
                rowId.intValue = mem.getInt(offset);
                offset += Integer.BYTES;
                // row_id could be
                // 1) st_separator: if the table is empty and there there are other
                // tables following;
                // 2) st_end: if the table is empty and there are no more table
                // following
                continue;
            }else if(rowId.intValue == GlobalContext.getSerializedTableEnd()){
                return  null;
            }else{
                tableId.intValue = currentTableId;
                int rowSize = mem.getInt(offset);
                offset += Integer.BYTES;
                mem.position(offset).limit(offset + rowSize);
                ByteBuffer dataMem = mem.slice();
                mem.clear();                //restore limit
                offset += rowSize;
                return dataMem;
            }
        }while(true);
    }
}
