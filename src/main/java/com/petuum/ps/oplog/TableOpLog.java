package com.petuum.ps.oplog;

import com.petuum.ps.common.Row;
import com.petuum.ps.common.oplog.RowOpLog;
import com.petuum.ps.common.util.BoolBox;
import com.petuum.ps.thread.GlobalContext;

import java.util.Map;
import java.util.Vector;

/**
 * Created by ZengJichuan on 2014/8/26.
 */
public class TableOpLog {
    private int tableId;
    private OpLogPartition[] opLogPartitions;

    public TableOpLog(int tableId, Row sampleRow) {
        this.tableId = tableId;
        this.opLogPartitions = new OpLogPartition[GlobalContext.getNumBgThreads()];
        for (int i = 0; i < GlobalContext.getNumBgThreads(); i++){
            opLogPartitions[i]= new OpLogPartition(sampleRow, tableId);
        }
    }

    public void inc(int rowId, int columnId, Double delta){
        int partitionNum = GlobalContext.getBgPartitionNum(rowId);
        opLogPartitions[partitionNum].inc(rowId, columnId, delta);
    }

    public void batchInc(int rowId, Map<Integer, Double> deltaBatch){
        int partitionNum = GlobalContext.getBgPartitionNum(rowId);
        opLogPartitions[partitionNum].batchInc(rowId, deltaBatch);
    }

    public RowOpLog findOpLog(int rowId){
        int partitionNum = GlobalContext.getBgPartitionNum(rowId);
        return opLogPartitions[partitionNum].findOpLog(rowId);
    }

    public RowOpLog findInsertOpLog(int rowId){
        int partitionNum = GlobalContext.getBgPartitionNum(rowId);
        return opLogPartitions[partitionNum].findInsertOpLog(rowId);
    }

    public RowOpLog getEraseOpLog(int rowId){
        int partitionNum = GlobalContext.getBgPartitionNum(rowId);
        return opLogPartitions[partitionNum].getEraseOpLog(rowId);
    }
}
