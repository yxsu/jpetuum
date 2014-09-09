package com.petuum.ps.thread;

/**
 * Created by ZengJichuan on 2014/8/21.
 */
public class TableRowIndex{
    public int tableId;
    public int rowId;

    public TableRowIndex(){}
    public TableRowIndex(int tableId, int rowId) {
        this.tableId = tableId;
        this.rowId = rowId;
    }
}
