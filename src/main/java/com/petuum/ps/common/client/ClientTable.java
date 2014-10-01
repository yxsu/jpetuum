package com.petuum.ps.common.client;
import com.petuum.ps.common.ClientTableConfig;
import com.petuum.ps.common.Row;
import com.petuum.ps.common.consistency.ConsistencyController;
import com.petuum.ps.common.consistency.SSPConsistencyController;
import com.petuum.ps.common.util.ClassRegistry;
import com.petuum.ps.oplog.TableOpLog;
import com.petuum.ps.oplog.TableOpLogIndex;
import com.petuum.ps.thread.GlobalContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @author Yuxin Su
 * @version 1.0
 * @created 19-??-2014 18:29:05
 */
public class ClientTable {

	private ConsistencyController consistencyController;
	private int rowType;
	private Row sampleRow;
	private int tableId;
	private ThreadLocal<ThreadTable> threadCache;
    private static Logger log = LogManager.getLogger(ClientTable.class);

    /**
     *
     * @param tableId
     * @param config
     */
    public ClientTable(int tableId, ClientTableConfig config) {
        this.rowType = config.tableInfo.rowType;
        ClassRegistry<Row> classRegistry = ClassRegistry.getRegistry();
        this.sampleRow = classRegistry.createObject(rowType);
        this.tableId = tableId;
        this.threadCache = new ThreadLocal<ThreadTable>();
        this.threadCache.set(new ThreadTable(this.sampleRow));

        switch (GlobalContext.getConsistencyModel()){
            case SSP:
            {
                consistencyController = new SSPConsistencyController(config.tableInfo, tableId, sampleRow,threadCache.get(),
                        config.processCacheCapacity);
                break;
            }
            case SSPPush:
            {
                //consistencyController = new SSPPushConsistencyController();
                break;
            }
            default:
                log.fatal("Not yet support consistency model " + GlobalContext.getConsistencyModel());
        }
    }

	/**
	 * 
	 * @exception Throwable
	 */
	public void finalize()
	  throws Throwable{

	}

	/**
	 * 
	 * @param row_id
	 * @param updates
	 */
	public void batchInc(int row_id, Map<Integer, Double> updates){
        consistencyController.batchInc(row_id, updates);
	}

	public void clock(){
        consistencyController.clock();
	}

	public void flushThreadCache(){
        consistencyController.flushThreadCache();
	}

	/**
	 * 
	 * @param rowId
	 */
	public Row get(int rowId){
        return consistencyController.get(rowId, true).getRowData();
    }

    public Row get(int rowId, int start, int offset) {
        return consistencyController.get(rowId, start, offset, true).getRowData();
    }

    public ClientRow getLocally(int rowId){
        return consistencyController.get(rowId, false);
    }

	public int getRowType(){
		return rowType;
	}

	public final Row getSampleRow(){
		return sampleRow;
	}

    public TableOpLog getOpLog(){
        return consistencyController.getOpLog();
    }
	/**
	 * 
	 * @param partitionNum    partition_num
	 */
	public Map<Integer, Boolean> getAndResetOpLogIndex(int partitionNum){
		return consistencyController.getAndResetOpLogIndex(partitionNum);
	}

	/**
	 * 
	 * @param rowId    row_id
	 */
	public void getAsync(int rowId){
        consistencyController.getAsync(rowId);
	}

	/**
	 * 
	 * @param rowId
	 * @param columnId
	 * @param update    update
	 */
	public void inc(int rowId, int columnId, Double update){
        consistencyController.inc(rowId, columnId, update);
	}

	public void registerThread(){
        if(threadCache.get() == null){
            threadCache.set(new ThreadTable(sampleRow));
        }
	}

	/**
	 * 
	 * @param rowId
	 * @param updates
	 */
	public void threadBatchInc(int rowId, Map<Integer, Double> updates){
        consistencyController.threadBatchInc(rowId, updates);
	}

	/**
	 * 
	 * @param rowId
	 */
	public Row threadGet(int rowId){
		return consistencyController.threadGet(rowId);
	}

	/**
	 * 
	 * @param row_id
	 * @param column_id
	 * @param update    update
	 */
	public void threadInc(int row_id, int column_id, Double update){
        consistencyController.threadInc(row_id, column_id, update);
	}

	public void waitPendingAsyncGet(){
        consistencyController.waitPendingAsnycGet();
	}

    /**
     *
     * @param rowId
     * @param clientRow
     */
    public void insert(int rowId, ClientRow clientRow) {
        consistencyController.insert(rowId, clientRow);
    }
}