package com.petuum.ps.common.test;

import com.petuum.ps.common.ClientTableConfig;
import com.petuum.ps.common.Row;
import com.petuum.ps.common.TableInfo;
import com.petuum.ps.common.client.ClientTable;
import com.petuum.ps.common.client.SSPClientRow;
import com.petuum.ps.common.storage.SparseRow;
import com.petuum.ps.thread.ClientConnectMsg;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ZengJichuan on 2014/9/3.
 */
public class TestStorage {
    public static void main(String args []){
        Row fRow = new SparseRow();
        fRow.applyInc(0, 1d);
        Map<Integer, Double> batch = new HashMap<Integer, Double>();
        batch.put(1,22d);batch.put(2,33d);batch.put(3, 44d);
        fRow.applyBatchInc(batch);
        System.out.println(((SparseRow)fRow).get(0)+" "+((SparseRow)fRow).get(1)+" "+((SparseRow)fRow).get(2)+" "+((SparseRow)fRow).get(3));

        TClientTable tc = new TClientTable();
        tc.rows.put(1, fRow);
        Row row = tc.rows.get(1);

        System.out.println(((SparseRow)row).get(0)+" "+((SparseRow)row).get(1));
    }
}

class TClientTable{
    public TClientTable(){
        rows = new HashMap<Integer, Row>();
    }
    public Map<Integer, Row> rows;
}