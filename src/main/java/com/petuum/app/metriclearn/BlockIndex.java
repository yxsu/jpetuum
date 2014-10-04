package com.petuum.app.metriclearn;

/**
 * Created by jjhu on 10/2/2014.
 */
public class BlockIndex{
    public BlockIndex(int globalWorkerId, int numBlockInRow){
        int threadId = globalWorkerId+1;
        for(int i = 0; i < numBlockInRow; i++){
            if(threadId <= numBlockInRow - i ){
                blockRowIndexi = i;
                blockColumnIndexj = i + threadId - 1;
                break;
            }else {
                threadId -= (numBlockInRow - i);
            }
        }
    }
    public int blockRowIndexi;
    public int blockColumnIndexj;
}
