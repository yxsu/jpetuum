package com.petuum.ps.common.client;

import com.petuum.ps.common.Row;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class aims to simplify the RowAccess and ClientRow.
 * @author Yuxin Su
 * @version 1.0
 * @created 19-??-2014 21:08:18
 */
public class ClientRow {

//	private AtomicInteger numRefs;
	private Row rowData;
    private int clock;

    public void setRowData(Row row) {
        rowData = row;
    }

	public void finalize() throws Throwable {

	}

	/**
	 * 
	 * @param clock
	 * @param rowData
	 */
	public ClientRow(int clock, Row rowData){
        this.rowData = rowData;
        this.clock = clock;
	}

	public void decRef(){

	}

	public int getClock(){
		return clock;
	}

	public Row getRowData(){
		return rowData;
	}

	public void incRef(){

	}

	/**
	 * 
	 * @param clock
	 */
	public void setClock(int clock){
        this.clock = clock;
	}

}