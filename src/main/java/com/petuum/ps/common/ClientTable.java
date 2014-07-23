package com.petuum.ps.common;

/**
 * Created by Su Yuxin on 2014/7/23.
 */
public interface ClientTable {
    void registerThread();
    void getAsync(int rowID);
    void waitPendingAsyncGet();
    void threadInc(int rowID, int columnID, Object[] update);
}
