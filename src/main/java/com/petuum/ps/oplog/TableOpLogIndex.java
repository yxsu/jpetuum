package com.petuum.ps.oplog;

import com.petuum.ps.thread.GlobalContext;

import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * Created by ZengJichuan on 2014/8/28.
 */
public class TableOpLogIndex {
    private Vector<PartitionOpLogIndex> partitionOpLogIndexes;

    public TableOpLogIndex() {
        partitionOpLogIndexes = new Vector<PartitionOpLogIndex>();
        for(int i = 0; i< GlobalContext.getNumBgThreads(); i++)
            partitionOpLogIndexes.add(new PartitionOpLogIndex());
    }

    public void addIndex(int partitionNum, Set<Integer> opLogIndex){
        partitionOpLogIndexes.get(partitionNum).addIndex(opLogIndex);
    }
    public Map<Integer, Boolean> resetPartition(int partitionNum){
        return partitionOpLogIndexes.get(partitionNum).reset();
    }
}
