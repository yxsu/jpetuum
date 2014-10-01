package com.petuum.ps.common.storage;

import com.google.common.util.concurrent.AtomicDouble;
import com.google.common.util.concurrent.AtomicDoubleArray;
import com.petuum.ps.common.Row;

import java.util.Map;
import java.util.Vector;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by ZengJichuan on 2014/9/2.
 */

public class DenseRow implements Row {

    private AtomicDoubleArray rowData;
    private int start;
    private int offset;

    public DenseRow() {
        
    }
    public DenseRow(AtomicDoubleArray rowData, int start, int offset) {
        this.rowData = rowData;
        this.start = start;
        this.offset = offset;
    }

    public Double addUpdates(int column_id, Double update1, Double update2) {
        return update1 + update2;
    }

    public void applyBatchInc(Map<Integer, Double> update_batch) {
        for(Map.Entry<Integer, Double> entry : update_batch.entrySet()) {
            rowData.getAndAdd(entry.getKey(), entry.getValue());
        }
    }

    public void applyBatchIncUnsafe(Map<Integer, Double> update_batch) {
        for(Map.Entry<Integer, Double> entry : update_batch.entrySet()) {
            rowData.getAndAdd(entry.getKey(), entry.getValue());
        }
    }

    public int getLength() {
        return rowData.length();
    }

    public Row getSegment(int start, int offset) {
        AtomicDoubleArray segmentData = new AtomicDoubleArray(offset);
        for(int i = 0; i < offset; i++) {
            segmentData.set(i, rowData.get(i + start));
        }
        return new DenseRow(segmentData, start, offset);
    }

    public int getStart() {
        return start;
    }

    public int getOffset() {
        return offset;
    }

    public void applyInc(int column_id, Double update) {
        rowData.getAndAdd(column_id, update);
    }

    public Double get(int columnId) {
        return rowData.get(columnId);
    }

    public void applyIncUnsafe(int column_id, Double update) {
        rowData.getAndAdd(column_id, update);
    }

    public int getUpdateSize() {
        return Double.BYTES;
    }

    public void init(int capacity) {
        rowData = new AtomicDoubleArray(capacity);
    }

    public void initUpdate(int column_id, Double zero) {
        rowData.set(column_id, zero);
    }

    public Double subtractUpdates(int column_id, Double update1, Double update2) {

        return update1 - update2;
    }
}
