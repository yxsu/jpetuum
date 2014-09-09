package com.petuum.ps.common;

import com.petuum.ps.common.TableInfo;

/**
 * ClientTableConfig is used by client only.
 * @author Yuxin Su
 * @version 1.0
 * @created 19-??-2014 15:36:42
 */
public class ClientTableConfig {

	/**
	 * Estimated upper bound # of pending oplogs in terms of # of rows. For SSP this
	 * is the # of rows all threads collectively touches in a Clock().
	 */
	public int opLogCapacity;
	/**
	 * In # of rows.
	 */
	public int processCacheCapacity;
	public TableInfo tableInfo;
	/**
	 * In # of rows.
	 */
	public int threadCacheCapacity;

	public ClientTableConfig(){
        this.tableInfo = new TableInfo();
	}

	public void finalize() throws Throwable {

	}

}