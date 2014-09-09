package com.petuum.ps.oplog;

import com.google.common.util.concurrent.Striped;
import com.petuum.ps.common.Row;
import com.petuum.ps.common.oplog.RowOpLog;
import com.petuum.ps.common.util.BoolBox;
import com.petuum.ps.thread.GlobalContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

/**
 * Created by ZengJichuan on 2014/8/25.
 */
public class OpLogPartition {
    private int updateSize;
    private HashMap<Integer, RowOpLog> opLogMap;
    private Striped<Lock> locks;
    private Row sampleRow;
    private int tableId;

    public OpLogPartition(Row sampleRow, int tableId) {
        this.opLogMap = new HashMap<Integer, RowOpLog>();
        this.locks = Striped.lock(GlobalContext.getLockPoolSize());
        this.sampleRow = sampleRow;
        this.tableId = tableId;
        this.updateSize = sampleRow.getUpdateSize();
    }

    public void inc(int rowId, int columnId, Double delta){
        Lock lock = locks.get(rowId);
        try{
            lock.lock();
            RowOpLog rowOpLog = opLogMap.get(rowId);
            if (rowOpLog == null){
                rowOpLog = createRowOpLog();
                opLogMap.put(rowId, rowOpLog);
            }
            Double opLogDelta = rowOpLog.findCreate(columnId, sampleRow);
            opLogDelta = sampleRow.addUpdates(columnId, opLogDelta, delta);
            rowOpLog.insert(columnId, opLogDelta);
        }finally {
            lock.unlock();
        }
    }

    private RowOpLog createRowOpLog() {
        RowOpLog rowOpLog = null;
        try {
            rowOpLog = new RowOpLog(Row.class.getMethod("initUpdate", int.class, Double.class));
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
        return rowOpLog;
    }

    public void batchInc(int rowId, Map<Integer, Double> deltaBatch){
        Lock lock = locks.get(rowId);
        try {
            lock.lock();

            RowOpLog rowOpLog = opLogMap.get(rowId);
            if (rowOpLog == null) {
                rowOpLog = createRowOpLog();
                opLogMap.put(rowId, rowOpLog);
            }
            for (Map.Entry<Integer, Double> entry : deltaBatch.entrySet()) {
                Double opLogDelta = rowOpLog.findCreate(entry.getKey(), sampleRow);
                opLogDelta = sampleRow.addUpdates(entry.getKey(), opLogDelta, entry.getValue());
                rowOpLog.insert(entry.getKey(), opLogDelta);        //replace
            }
        }finally {
            lock.unlock();
        }
    }

    public RowOpLog findOpLog(int rowId){
        return opLogMap.get(rowId);
    }

    public RowOpLog findInsertOpLog(int rowId){
        Lock lock = locks.get(rowId);
        RowOpLog rowOpLog = null;
        try {
            lock.lock();
            rowOpLog = opLogMap.get(rowId);
            if (rowOpLog == null){
                rowOpLog = createRowOpLog();
                opLogMap.put(rowId, rowOpLog);
            }
        }finally {
            lock.unlock();
        }
        return rowOpLog;
    }

    /**
     *
     * @param rowId
     * @return null means not that item
     */
    public RowOpLog getEraseOpLog(int rowId){
        Lock lock = locks.get(rowId);
        RowOpLog rowOpLog = null;
        try {
            lock.lock();
            rowOpLog = opLogMap.remove(rowId);
        }finally {
            lock.unlock();
        }
        return rowOpLog;
    }

    //GetEraseOpLogIf
}
