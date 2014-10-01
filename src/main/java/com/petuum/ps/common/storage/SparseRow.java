package com.petuum.ps.common.storage;

import com.petuum.ps.common.Row;
import org.apache.commons.lang3.SerializationUtils;

import javax.swing.text.html.parser.Entity;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by ZengJichuan on 2014/9/2.
 */
public class SparseRow implements Row, Iterable<Map.Entry<Integer, Double>> {

    private ReadWriteLock lock;

//    private int updateSize;

    ConcurrentMap<Integer, Double> rowData;
    private int start;
    private int offset;

    public SparseRow() {
        this.lock = new ReentrantReadWriteLock();
//        this.updateSize = SerializationUtils.serialize(sampleElem).length;
        this.rowData = new ConcurrentHashMap<Integer, Double>();
    }

    public int getLength() {
        return rowData.size();
    }

    public Double get(int columnId){
        try {
            lock.readLock().lock();
            return rowData.getOrDefault(columnId, 0d);
        }finally {
            lock.readLock().unlock();
        }
    }

    public Row getSegment(int start, int offset) {
        SparseRow subRow = new SparseRow();
        subRow.start = this.start;
        subRow.offset = this.offset;
        int end = start + offset;
        try {
            lock.readLock().lock();
            for(Map.Entry<Integer, Double> entry : rowData.entrySet()) {
                if(entry.getKey() >= start && entry.getKey() < end) {
                    subRow.applyInc(entry.getKey(), entry.getValue());
                }
            }
        }finally {
            lock.readLock().unlock();
        }
        return subRow;
    }

    public int numEntries(){
        try{
            lock.readLock().lock();
            return rowData.size();
        }finally {
            lock.readLock().unlock();
        }
    }

    public int getStart() {
        return start;
    }

    public int getOffset() {
        return offset;
    }

    public Double addUpdates(int column_id, Double update1, Double update2) {
        // Ignore column_id
        return update1 + update2;
    }

    public void applyBatchInc(Map<Integer, Double> updateBatch) {
        try{
            lock.writeLock().lock();
            applyBatchIncUnsafe(updateBatch);
        }finally {
            lock.writeLock().unlock();
        }

    }

    public void applyBatchIncUnsafe(Map<Integer, Double> updateBatch) {
        for (Map.Entry<Integer, Double> entry : updateBatch.entrySet()){
            int columnId = entry.getKey();
            rowData.put(columnId, rowData.getOrDefault(columnId, 0d) + entry.getValue());
            if (Math.abs(rowData.get(columnId)) < 1e-9){
                rowData.remove(columnId);
            }
        }
    }

    public void applyInc(int columnId, Double update) {
        try {
            lock.writeLock().lock();
            applyIncUnsafe(columnId, update);
        }finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * convert Object to Double, then to double and plus
     * @param column_id
     * @param update
     */
    public void applyIncUnsafe(int column_id, Double update) {
        rowData.put(column_id, update + rowData.getOrDefault(column_id, 0d));
        if(Math.abs(rowData.get(column_id)) < 1e-9){
            rowData.remove(column_id);
        }
    }

    /**
     * the size of Double is not what you can see, i.e. Integer:81, Short:77, Double:84, Float:79
     * @return
     */
    public int getUpdateSize() {
        return Double.BYTES;
    }

    public void init(int capacity) {
    }

    public void initUpdate(int column_id, Double zero) {

    }


    public Double subtractUpdates(int column_id, Double update1, Double update2) {
        // Ignore column_id
        return update1 - update2;
    }

    // ======== const_iterator Implementation ========
    public Iterator<Map.Entry<Integer, Double>> iterator() {
        final Iterator<Map.Entry<Integer, Double>> iter = new Iterator<Map.Entry<Integer, Double>>() {
            private Iterator<Map.Entry<Integer, Double>> mapIter = rowData.entrySet().iterator();
            public boolean hasNext() {
                return mapIter.hasNext();
            }

            public Map.Entry<Integer, Double> next() {
                return mapIter.next();
            }
        };
        return iter;
    }
}
