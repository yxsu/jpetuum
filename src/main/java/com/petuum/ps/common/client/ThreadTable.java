package com.petuum.ps.common.client;
import com.google.common.cache.Cache;
import com.petuum.ps.common.Row;
import com.petuum.ps.common.oplog.RowOpLog;
import com.petuum.ps.common.util.IntBox;
import com.petuum.ps.oplog.TableOpLog;
import com.petuum.ps.oplog.TableOpLogIndex;
import com.petuum.ps.thread.GlobalContext;
import org.apache.commons.lang3.SerializationUtils;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * @author Yuxin Su
 * @version 1.0
 * @created 19-??-2014 18:11:45
 */
public class ThreadTable {

	private Vector<ConcurrentSkipListSet<Integer>> opLogIndex;
    private Map<Integer, RowOpLog> opLogMap;
	private Map<Integer, Row> rowStorage;
	private Row sampleRow;

	public ThreadTable(Row sampleRow){
        this.sampleRow = sampleRow;
        opLogIndex = new Vector<ConcurrentSkipListSet<Integer>>();
        for(int i = 0; i < GlobalContext.getNumBgThreads(); i++){
            opLogIndex.add(new ConcurrentSkipListSet<Integer>());
        }
        rowStorage = new HashMap<Integer, Row>();
        opLogMap = new HashMap<Integer, RowOpLog>();
	}

	/**
	 * 
	 * @param row_id
	 * @param deltas
	 */
	public void batchInc(int row_id, final Map<Integer, Double> deltas) {
        RowOpLog rowOpLog = null;
        try {
            rowOpLog = opLogMap.getOrDefault(row_id,
                    new RowOpLog(Row.class.getMethod("initUpdate", int.class, Double.class)));
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }

        for(Map.Entry<Integer, Double> entry : deltas.entrySet()) {
            Double oplogDelta = rowOpLog.findCreate(entry.getKey(), sampleRow);
            rowOpLog.insert(entry.getKey(), oplogDelta + entry.getValue());
        }

        Row row = rowStorage.get(row_id);
        if(row != null) {
            row.applyBatchIncUnsafe(deltas);
        }
	}

	/**
	 * 
	 * @param rowId
	 */
	public Row getRow(int rowId){
		return rowStorage.get(rowId);
	}

	/**
	 * 
	 * @param row_id
	 * @param column_id
	 * @param delta
	 */
	public void inc(int row_id, int column_id, final Double delta) {

        RowOpLog rowOpLog = null;
        try {
            rowOpLog = opLogMap.getOrDefault(row_id,
                    new RowOpLog(Row.class.getMethod("initUpdate", int.class, Double.class)));
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
        Double oplogDelta = rowOpLog.findCreate(column_id, sampleRow);
        rowOpLog.insert(column_id, oplogDelta + delta);
        Row row = rowStorage.get(row_id);
        if(row != null) {
            row.applyIncUnsafe(column_id, delta);
        }
	}

	/**
	 * 
	 * @param rowId
	 */
	public void indexUpdate(int rowId){
        int partitionNum = GlobalContext.getBgPartitionNum(rowId);
        opLogIndex.get(partitionNum).add(rowId);
	}

	/**
	 * 
	 * @param rowId
	 * @param toInsert
	 */
	public void insertRow(int rowId, final Row toInsert){
        //clone
        Row copyRow = SerializationUtils.clone(toInsert);
        rowStorage.putIfAbsent(rowId, copyRow);
        RowOpLog rowOpLog = opLogMap.get(rowId);
        if(rowOpLog != null) {
            IntBox columnId = new IntBox();
            Double delta = rowOpLog.beginIterate(columnId);
            while (delta != null){
                copyRow.applyInc(columnId.intValue, delta);
                delta = rowOpLog.next(columnId);
            }
        }

	}

    public void flushCache(Cache<Integer, ClientRow> processStorage, TableOpLog opLog, Row sampleRow) {

        for(Map.Entry<Integer, RowOpLog> entry : opLogMap.entrySet()) {
            int rowId = entry.getKey();
            int partitionNum = GlobalContext.getBgPartitionNum(rowId);

            RowOpLog rowOpLog = opLog.findInsertOpLog(rowId);
            ClientRow clientRow = processStorage.getIfPresent(rowId);
            IntBox columnId = new IntBox();
            Double delta = entry.getValue().beginIterate(columnId);
            while(delta != null) {
                Double oplogDelta = rowOpLog.findCreate(columnId.intValue, sampleRow);
                rowOpLog.insert(columnId.intValue, oplogDelta + delta);
                opLogIndex.get(partitionNum).add(rowId);
                if(clientRow != null) {
                    clientRow.getRowData().applyInc(columnId.intValue, delta);
                }
                delta = entry.getValue().next(columnId);
            }
        }
        opLogMap.clear();
        rowStorage.clear();
    }

    public void flushOpLogIndex(TableOpLogIndex tableOpLogIndex) {
        for (int i = 0; i < GlobalContext.getNumBgThreads(); i++) {
            tableOpLogIndex.addIndex(i, opLogIndex.get(i));
            opLogIndex.get(i).clear();
        }
    }
}