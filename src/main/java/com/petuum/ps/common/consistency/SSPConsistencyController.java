package com.petuum.ps.common.consistency;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.petuum.ps.common.Constants;
import com.petuum.ps.common.Row;
import com.petuum.ps.common.TableInfo;
import com.petuum.ps.common.client.ClientRow;
import com.petuum.ps.common.client.ThreadTable;
import com.petuum.ps.common.oplog.RowOpLog;
import com.petuum.ps.oplog.TableOpLog;
import com.petuum.ps.oplog.TableOpLogIndex;
import com.petuum.ps.thread.BgWorkers;
import com.petuum.ps.thread.ThreadContext;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

/**
 * @author Su Yuxin
 * @version 1.0
 * @created 19-??-2014 20:18:33
 */
public class SSPConsistencyController extends ConsistencyController {

	/**
	 * SSP staleness parameter.
	 */
	protected int staleness;
	protected ThreadTable threadCache;
    /**
     * Controller will only write to oplog_ but never read from it, as
     * all local updates are reflected in the row values.
     */
    protected TableOpLog opLog;
    protected TableOpLogIndex opLogIndex;

	public void finalize() throws Throwable {
		super.finalize();
	}

	/**
	 * 
	 * @param tableId
	 * @param sampleRow
	 * @param threadCache
	 */
	public SSPConsistencyController(TableInfo info, int tableId, final Row sampleRow, ThreadTable threadCache, int cacheSize){
        this.threadCache = threadCache;
        this.sampleRow = sampleRow;
        this.sampleRow.init(info.rowCapacity);
        this.tableId = tableId;
        this.opLog = new TableOpLog(tableId, sampleRow);
        this.opLogIndex = new TableOpLogIndex();
        this.processStorage = CacheBuilder.newBuilder().
                maximumSize((long) Math.ceil(cacheSize / Constants.HASH_MAP_LOAD_FACTOR)).build();
	}

	/**
	 * 
	 * @param rowId
	 * @param updates
	 */
	public void batchInc(int rowId, Map<Integer, Double> updates){
        threadCache.indexUpdate(rowId);
        RowOpLog rowOpLog = opLog.findInsertOpLog(rowId);
        for (Map.Entry<Integer, Double> entry : updates.entrySet()){
            Double opLogDelta = rowOpLog.findCreate(entry.getKey(), sampleRow);
            opLogDelta = sampleRow.addUpdates(entry.getKey(), opLogDelta, entry.getValue());
            rowOpLog.insert(entry.getKey(), opLogDelta);        //replace the old
        }

        ClientRow clientRow = processStorage.getIfPresent(rowId);
        if (clientRow != null){
            clientRow.getRowData().applyBatchInc(updates);
        }
    }

	public void clock(){
        // order is important
        threadCache.flushCache(processStorage, opLog, sampleRow);
        threadCache.flushOpLogIndex(opLogIndex);
    }

	public void flushThreadCache(){
        threadCache.flushCache(processStorage, opLog, sampleRow);
    }

	/**
	 * 
	 * @param rowId
	 */
	public ClientRow get(int rowId, boolean fetchFromServer) { //how to get the clock? use a list
        int stalestClock = ThreadContext.getClock() - staleness;
        ClientRow clientRow = processStorage.getIfPresent(rowId);
        if (clientRow != null){
            //found it! Check staleness
            int clock = clientRow.getClock();
            if (clock >= stalestClock){
                return clientRow;
            }
        }
        if (fetchFromServer == false)   return clientRow;           //skip the fetch process, return null
        // Didn't find row_id that's fresh enough in process_storage_.
        // Fetch from server.
        do {
            BgWorkers.requestRow(tableId, rowId, stalestClock);
            clientRow = processStorage.getIfPresent(rowId);
        }while(clientRow == null);
        Preconditions.checkArgument(clientRow.getClock() >= stalestClock);
        return clientRow;
    }


	/**
	 * 
	 * @param rowId
	 */
	public void getAsync(int rowId) {

    }

	/**
	 * 
	 * @param rowId
	 * @param columnId
	 * @param delta
	 */
	public void inc(int rowId, int columnId, Double delta) {
        threadCache.indexUpdate(rowId);
        RowOpLog rowOpLog = opLog.findInsertOpLog(rowId);
        Double opLogDelta = rowOpLog.findCreate(columnId, sampleRow);
        opLogDelta = sampleRow.addUpdates(columnId, opLogDelta, delta);
        rowOpLog.insert(columnId, opLogDelta);          //replace
        //update to process_storage
        ClientRow clientRow = processStorage.getIfPresent(rowId);
        if (clientRow != null){
            clientRow.getRowData().applyInc(columnId, delta);
        }
    }

	/**
	 * 
	 * @param rowId
	 * @param updates
	 */
	public void threadBatchInc(int rowId, Map<Integer, Double> updates){
        threadCache.batchInc(rowId, updates);
    }

	/**
	 * 
	 * @param rowId
	 */
	public Row threadGet(int rowId){
        Row rowData = threadCache.getRow(rowId);
        if (rowData != null){
            return rowData;
        }

        ClientRow clientRow = processStorage.getIfPresent(rowId);
        int stalestClock = Math.max(0, ThreadContext.getClock() - staleness);
        if(clientRow != null){
            int clock = clientRow.getClock();
            if (clock >= stalestClock){
                threadCache.insertRow(rowId, clientRow.getRowData());
                return Preconditions.checkNotNull(threadCache.getRow(rowId));
            }
        }
        // Didn't find row_id that's fresh enough in process_storage_.
        // Fetch from server.
        do {
            BgWorkers.requestRow(tableId, rowId, stalestClock);
            clientRow = processStorage.getIfPresent(rowId);
            try {
                Thread.sleep(0, 500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }while(clientRow == null);
        Preconditions.checkArgument(clientRow.getClock() >= stalestClock);
        threadCache.insertRow(rowId, clientRow.getRowData());
        return Preconditions.checkNotNull(threadCache.getRow(rowId));
    }

	/**
	 * 
	 * @param rowId
	 * @param columnId
	 * @param delta
	 */
	public void threadInc(int rowId, int columnId, Double delta){
        threadCache.inc(rowId, columnId, delta);
    }

	public void waitPendingAsnycGet(){

    }

    public Map<Integer, Boolean> getAndResetOpLogIndex(int clientTable){
        return opLogIndex.resetPartition(clientTable);
    }

    @Override
    public TableOpLog getOpLog() {
        return opLog;
    }

    @Override
    public void insert(int rowId, ClientRow clientRow) {
        processStorage.put(rowId, clientRow);
    }
}