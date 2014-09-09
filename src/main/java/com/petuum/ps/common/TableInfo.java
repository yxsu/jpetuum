package com.petuum.ps.common;

/**
 * TableInfo is shared between client and server.
 * @author Yuxin Su
 * @version 1.0
 * @created 19-??-2014 15:40:18
 */
public class TableInfo {

	/**
	 * row_capacity can mean different thing for different row_type. For example in
	 * vector-backed dense row it is the max number of columns. This parameter is
	 * ignored for sparse row.
	 */
	public int rowCapacity;
	/**
	 * A table can only have one type of row. The row_type is defined when calling
	 * TableGroup::RegisterRow().
	 */
	public int rowType;
	/**
	 * table_staleness is used for SSP and ClockVAP.
	 */
	public int tableStaleness;

	public TableInfo(){

	}

	public void finalize() throws Throwable {

	}

}