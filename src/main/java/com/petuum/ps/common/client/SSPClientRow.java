package com.petuum.ps.common.client;

import com.petuum.ps.common.Row;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by ZengJichuan on 2014/9/2.
 */
public class SSPClientRow extends ClientRow {
    private Lock clockLock;
    int clock;

    public SSPClientRow(int clock, Row rowData) {
        super(clock, rowData);
        this.clock = clock;
        this.clockLock = new ReentrantLock();
    }

    public void setClock(int clock){
        clockLock.lock();
        try {
            this.clock = clock;
        }finally {
            clockLock.unlock();
        }
    }

    public int getClock(){
        clockLock.lock();
        int clockR;
        try{
            clockR = this.clock;
        }finally {
            clockLock.unlock();
        }
        return clockR;
    }

//    public void swapAndDestory
}
