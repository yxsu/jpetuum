package com.petuum.ps.common.util;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by admin on 2014/8/13.
 */
public class VectorClockMT extends VectorClock {
    private Lock lock = new ReentrantLock();
    @Override
    public void addClock(int id, int clock){
        lock.lock();
        try {
            super.addClock(id, clock);
        }finally {
            lock.unlock();
        }
    }
    @Override
    public int tick(int id){
        lock.lock();
        try {
            return super.tick(id);
        }finally {
            lock.unlock();
        }
    }
    @Override
    public int getClock(int id){
        lock.lock();
        try {
            return super.getClock(id);
        }finally {
            lock.unlock();
        }
    }
    @Override
    public int getMinClock(){
        lock.lock();
        try {
            return super.getMinClock();
        }finally {
            lock.unlock();
        }
    }
}
