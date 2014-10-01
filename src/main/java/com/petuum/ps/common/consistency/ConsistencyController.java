package com.petuum.ps.common.consistency;

import com.google.common.cache.Cache;
import com.google.common.cache.LoadingCache;
import com.petuum.ps.common.Row;
import com.petuum.ps.common.client.ClientRow;
import com.petuum.ps.oplog.TableOpLog;

import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Interface for consistency controller modules. For each table we associate a
 * consistency controller (e.g., SSPController) that's essentially the "brain"
 * that maintains a prescribed consistency policy upon each table action. All
 * functions should be fully thread-safe.
 * @author Yuxin Su
 * @version 1.0
 * @updated 19-??-2014 14:25:28
 */
public abstract class ConsistencyController {

	/**
	 * common class members for all controller modules. Process cache, highly
	 * concurrent.
	 */
	protected Cache<Integer, ClientRow> processStorage;
	/**
	 * We use sample_row_.AddUpdates(), SubstractUpdates() as static method.
	 */
	protected Row sampleRow;
	protected int tableId;

	public ConsistencyController(){

	}

	/**
	 * 
	 * @param tableID
	 * @param sample_row    sample_row
	 */
	public ConsistencyController(int tableID, final Row sample_row){

	}

	/**
	 * 
	 * @exception Throwable Throwable
	 */
	public void finalize()
	  throws Throwable{

	}

	/**
	 * 
	 * @param row_id
	 * @param updates    updates
	 */
	public abstract void batchInc(int row_id,  Map<Integer, Double> updates);

	public abstract void clock();

	public abstract void flushThreadCache();

	/**
	 * 
	 * @param row_id    row_id
	 */
	public abstract ClientRow get(int row_id, boolean fetchFromServer);

    public abstract ClientRow get(int rowId, int start, int offset, boolean fetchFromServer);

	/**
	 * 
	 * @param row_id    row_id
	 */
	public abstract void getAsync(int row_id);

	/**
	 * 
	 * @param row_id
	 * @param column_id
	 * @param delta    delta
	 */
	public abstract void inc(int row_id, int column_id, final Double delta);

	/**
	 * 
	 * @param row_id
	 * @param updates    updates
	 */
	public abstract void threadBatchInc(int row_id, final Map<Integer, Double> updates);

	/**
	 * 
	 * @param row_id    row_id
	 */
	public abstract Row threadGet(int row_id);

	/**
	 * 
	 * @param row_id
	 * @param column_id
	 * @param delta    delta
	 */
	public abstract void threadInc(int row_id, int column_id, final Double delta);

	public abstract void waitPendingAsnycGet();

    public abstract Map<Integer,Boolean> getAndResetOpLogIndex(int clientTable);

    public abstract TableOpLog getOpLog();

    public abstract void insert(int rowId, ClientRow clientRow);
}