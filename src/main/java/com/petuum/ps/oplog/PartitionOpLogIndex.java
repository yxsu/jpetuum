package com.petuum.ps.oplog;

import com.google.common.util.concurrent.Striped;
import com.petuum.ps.thread.GlobalContext;
import com.sun.org.apache.xpath.internal.operations.Bool;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by ZengJichuan on 2014/8/28.
 */
public class PartitionOpLogIndex {
    private ReadWriteLock sharedLock;
    private Striped<Lock> locks;
    private HashMap<Integer, Boolean> sharedOpLogIndex;

    public PartitionOpLogIndex(){
        sharedLock = new ReentrantReadWriteLock();
        locks = Striped.lock(GlobalContext.getLockPoolSize());
        sharedOpLogIndex = new HashMap<Integer, Boolean>();
    }
    public void addIndex(Set<Integer> opLogIndex){
        try {
            sharedLock.readLock().lock();
       
            Iterator<Integer> iter = opLogIndex.iterator();
            while(iter.hasNext()){
                int index = iter.next();
                Lock lock = locks.get(index);
                try {
                    lock.lock();
                    sharedOpLogIndex.put(index, true);
                } finally {
                    lock.unlock();
                }
            }
        }catch (Exception e) {
            e.printStackTrace();
        }finally {
            sharedLock.readLock().unlock();
        }
    }
    public HashMap<Integer, Boolean> reset() {
        HashMap<Integer, Boolean> oldIndex;
        try {
            sharedLock.writeLock().lock();
            oldIndex = sharedOpLogIndex;
            sharedOpLogIndex = new HashMap<Integer, Boolean>();
        }finally {
            sharedLock.writeLock().unlock();
        }
        return oldIndex;
    }
}
