package com.petuum.ps.thread;

import com.petuum.ps.common.oplog.RowOpLog;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zjc on 2014/8/14.
 */
public class BgOpLogPartition {
    private Map<Integer, RowOpLog> opLogMap;
    private int tableId;
    private int updateSize;

    public BgOpLogPartition(int tableId, int updateSize) {
        this.tableId = tableId;
        this.updateSize = updateSize;
        this.opLogMap = new HashMap<Integer, RowOpLog>();
    }
    public RowOpLog findOpLog(int rowId){
        return opLogMap.get(rowId);
    }
    public void insertOpLog(int rowId, RowOpLog rowOpLog){
        opLogMap.put(rowId, rowOpLog);
    }

    public void serializedByServer(Map<Integer, ByteBuffer> bufferByServer) {
        Map<Integer, Integer> offsetByServer = new HashMap<Integer, Integer>();
        // init number of rows to 0
        for (int serverId : GlobalContext.getServerIds()){
            offsetByServer.put(serverId, Integer.BYTES);
            bufferByServer.get(serverId).putInt(0);
        }
        for (Map.Entry<Integer, RowOpLog> entry : opLogMap.entrySet()){
            int rowId = entry.getKey();
            int serverId = GlobalContext.getRowPartitionServerId(tableId, rowId);
            RowOpLog rowOpLog = entry.getValue();

            ByteBuffer mem = bufferByServer.get(serverId);
            mem.position(offsetByServer.get(serverId));

            mem.putInt(rowId);                          //rowId
            //Serialize the RowOpLog
            byte[] rowOpLogBytes = rowOpLog.serialized();

            int rowOpLogSize = rowOpLogBytes.length;
            mem.putInt(rowOpLogSize);             //opLog mem size
            mem.put(rowOpLogBytes);                 //opLog mem

            offsetByServer.put(serverId, offsetByServer.get(serverId) + Integer.BYTES + Integer.BYTES +
                    rowOpLogSize);
            mem.putInt(0, mem.getInt(0)+1);         //row num add 1
        }
    }
}
