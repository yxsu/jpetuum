package com.petuum.ps.common;

import java.io.Serializable;
import java.util.Map;

/**
 * This class defines the interface of the Row type.  ApplyUpdate() and
 * ApplyBatchUpdate() have to be concurrent with each other and with other
 * functions that may be invoked by application threads.  Petuum system does not
 * require thread safety of other functions.
 * @author Yuxin Su
 * @version 1.0
 * @created 19-??-2014 14:29:06
 */
public abstract interface Row extends Serializable{

//	public void finalize() throws Throwable;

	/**
	 * Aggregate update1 and update2 by summation and substraction (update1 - update2),
	 * outputing to update2. column_id is optionally used in case updates are applied
	 * differently for different column of a row. Both AddUpdates and SubstractUpdates
	 * should behave like a static method.  But we cannot have virtual static method.
	 * Need be thread-safe and better be concurrent.
	 * 
	 * @param column_id
	 * @param update1
	 * @param update2
	 */
	public abstract Double addUpdates(int column_id, Double update1, final Double update2);

	/**
	 * 
	 * @param update_batch
	 */
	public abstract void applyBatchInc(Map<Integer, Double> update_batch);

	/**
	 * 
	 * @param update_batch
	 */
	public abstract void applyBatchIncUnsafe(final Map<Integer, Double> update_batch);

	/**
	 * 
	 * @param column_id
	 * @param update
	 */
	public abstract void applyInc(int column_id, final Double update);

	/**
	 * 
	 * @param column_id
	 * @param update
	 */
	public abstract void applyIncUnsafe(int column_id, final Double update);

    public abstract int getLength();

    public abstract Row getSegment(int start, int offset);

    public abstract int getStart();

    public abstract int getOffset();

	public abstract int getUpdateSize();

	/**
	 * 
	 * @param capacity
	 */
	public abstract void init(int capacity);

	/**
	 * Initialize update. Initialized update represents "zero update". In other words,
	 * 0 + u = u (0 is the zero update).
	 * 
	 * @param column_id
	 * @param zero
	 */
	public abstract void initUpdate(int column_id, Double zero);

	/**
	 * 
	 * @param column_id
	 * @param update1
	 * @param update2
	 */
	public abstract Double subtractUpdates(int column_id, Double update1, final Double update2);

    public abstract Double get(int columnId);

}