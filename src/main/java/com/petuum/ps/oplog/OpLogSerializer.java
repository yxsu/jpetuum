package com.petuum.ps.oplog;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ZengJichuan on 2014/8/27.
 */
public class OpLogSerializer {

    private Map<Integer, Integer> offsetMap;
    private ByteBuffer mem;
    private int numTables;

    public OpLogSerializer(){
        offsetMap = new HashMap<Integer, Integer>();
    }

    public int init(Map<Integer, Integer> tableSizeMap){
        numTables = tableSizeMap.size();
        // space for num of tables
        int totalSize = Integer.BYTES;
        for (Map.Entry<Integer, Integer> entry : tableSizeMap.entrySet()){
            int tableId = entry.getKey();
            int tableSize = entry.getValue();
            offsetMap.put(tableId, totalSize);
            // next table is offset by
            // 1) the current table size and
            // 2) space for table id
            // 3) update size
            totalSize += tableSize + Integer.BYTES + Integer.BYTES;
        }
        return totalSize;
    }

    //just putInt(numTables)
    public void assignMem(ByteBuffer buffer){
        buffer.putInt(numTables);
    }

    public int getTablePos(int tableId){
        return offsetMap.get(tableId);
    }


}
