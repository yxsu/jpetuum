package com.petuum.app.metriclearn;

import jdk.nashorn.internal.ir.Block;

/**
 * Created by jjhu on 9/30/2014.
 */
public class MetricLearnBlock{
    MetricLearnBlock(int i, int j, int dimData, int numBlockInRow){
        this.blockRowIndexi = i;
        this.blockColumnIndexj = j;
        this.dimData = dimData;
        this.numBlockInRow = numBlockInRow;
        this.globalWorkerId = blockToWorkerId(i,j);

        int remainder = dimData % numBlockInRow;
        int dimBlock = dimData / numBlockInRow;
        dimBlock = (remainder == 0)? dimBlock : dimBlock+1;
        this.isDiagonal = (blockRowIndexi == blockColumnIndexj);
        this.rowSize = (blockRowIndexi == numBlockInRow -1)? dimData - (numBlockInRow - 1) * dimBlock : dimBlock;
        this.columnSize = (blockColumnIndexj == numBlockInRow -1)? dimData - (numBlockInRow - 1) * dimBlock : dimBlock;
        this.startRow = blockRowIndexi * dimBlock;
        this.startColumn = blockColumnIndexj * dimBlock;

        if(i <= j) {
            stepRow = columnSize;
            stepColumn = 1;
        }else {
            stepRow = 1;
            stepColumn = rowSize;
        }
    }
    public static int blockToWorkerId(int blockRowIndexI, int blockColumnIndexJ){
        if(blockRowIndexI > blockColumnIndexJ){
            int temp = blockRowIndexI;
            blockRowIndexI = blockColumnIndexJ;
            blockColumnIndexJ = temp;
        }
        return ( 2 * numBlockInRow - blockRowIndexI + 1) * blockRowIndexI / 2 + blockColumnIndexJ - blockRowIndexI;
    }


    public String toString(){
        String s = "isDiagonal: "+(isDiagonal?"true\n":"false\n");
        s += "block index i : "+blockRowIndexi+"\n";
        s += "block col index j : " +blockColumnIndexj + "\n";
        s += "row size :" + rowSize+ "\n";
        s += "column size:" + columnSize+ "\n";
        //s += "start row:" + startRow+ "\n";
        //s += "start column:" + startColumn+ "\n";
        return s;
    }
    public int globalWorkerId;
    public static int numBlockInRow;
    public int dimData;
    public boolean isDiagonal;
    public int blockRowIndexi;
    public int blockColumnIndexj;
    public int rowSize;
    public int columnSize;
    public int stepRow;
    public int stepColumn;
    public int startRow;
    public int startColumn;
}
