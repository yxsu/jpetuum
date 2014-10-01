package com.petuum.app.metriclearn;

/**
 * Created by jjhu on 9/30/2014.
 */
public class MetricLearnBlock{
    MetricLearnBlock(int _globalWorkerId, int _numBlockInRow, int _dimData){
        globalWorkerId = _globalWorkerId;
        numBlockInRow = _numBlockInRow;
        dimData = _dimData;

        // mapping from globalWorkerId to block indexs
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
        int remainder = dimData % numBlockInRow;
        int dimBlock = dimData / numBlockInRow;
        dimBlock = (remainder == 0)? dimBlock : dimBlock+1;
        isDiagonal = (blockRowIndexi == blockColumnIndexj);
        rowSize = (blockRowIndexi == numBlockInRow -1)? dimData - (numBlockInRow - 1) * dimBlock : dimBlock;
        columnSize = (blockColumnIndexj == numBlockInRow -1)? dimData - (numBlockInRow - 1) * dimBlock : dimBlock;
        startRow = blockRowIndexi * dimBlock;
        startColumn = blockColumnIndexj * dimBlock;
    }
    public String toString(){
        String s = "isDiagonal: "+(isDiagonal?"true\n":"false\n");
        s += "block index i : "+blockRowIndexi+"\n";
        s += "block col index j : " +blockColumnIndexj + "\n";
        s += "row size :" + rowSize+ "\n";
        s += "column size:" + columnSize+ "\n";
        s += "start row:" + startRow+ "\n";
        s += "start column:" + startColumn+ "\n";
        return s;
    }
    public int globalWorkerId;
    public int numBlockInRow;
    public int dimData;
    public boolean isDiagonal;
    public int blockRowIndexi;
    public int blockColumnIndexj;
    public int rowSize;
    public int columnSize;
    public int startRow;
    public int startColumn;
}
