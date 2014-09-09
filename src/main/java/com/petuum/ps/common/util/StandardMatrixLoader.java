package com.petuum.ps.common.util;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Vector;
import java.util.function.Consumer;

/**
 * Created by suyuxin on 14-9-3.
 */
public class StandardMatrixLoader implements MatrixLoader{
    private int n_;
    private int m_;
    private Vector<Integer> xRow = new Vector<Integer>();
    private Vector<Integer> xCol = new Vector<Integer>();
    private Vector<Float> xVal = new Vector<Float>();

    private int numWorkers;
    private int[] workerNextElPos;

    public StandardMatrixLoader(Path inputFile, int numWorkers) throws IOException {
        this.numWorkers = numWorkers;
        this.workerNextElPos = new int[numWorkers];
        // Initialize workers to start of data
        for(int i = 0; i < numWorkers; i++) {
            workerNextElPos[i] = i;
        }
        //Load data
        readSparseMatrix(inputFile);
    }

    /*
   * Read sparse data matrix into X_row, X_col and X_val. Each line of the matrix
   * is a whitespace-separated triple (row,col,value), where row>=0 and col>=0.
   * For example:
   *
   * 0 0 0.5
   * 1 2 1.5
   * 2 1 2.5
   *
   * This specifies a 3x3 matrix with 3 nonzero elements: 0.5 at (0,0), 1.5 at
   * (1,2) and 2.5 at (2,1).
   */
    public void readSparseMatrix(Path inputFile) throws IOException {
        xRow.clear();
        xCol.clear();
        xVal.clear();
        n_ = 0;
        m_ = 0;
        Files.lines(inputFile, StandardCharsets.US_ASCII).forEach(new Consumer<String>() {
            public void accept(String s) {
                String[] temp = s.split(" ");
                int row = Integer.valueOf(temp[0]);
                int col = Integer.valueOf(temp[1]);
                xRow.add(row);
                xCol.add(col);
                xVal.add(Float.valueOf(temp[2]));
                n_ = row + 1 > n_ ? row + 1 : n_;
                m_ = col + 1 > m_ ? col + 1 : m_;
            }
        });
    }

    public Element getNextEl(int workerId) {
        int dataId = workerNextElPos[workerId];
        Element result = new Element();
        result.row = xRow.get(dataId);
        result.col = xCol.get(dataId);
        result.value = xVal.get(dataId);
        //Advance to next element
        workerNextElPos[workerId] = workerNextElPos[workerId] + numWorkers;
        result.isLastEl = false;
        if(workerNextElPos[workerId] >= getNNZ()) {
            //return to start of data
            workerNextElPos[workerId] = workerId;
            result.isLastEl = true;
        }
        return result;
    }

    public int getN() {
        return n_;
    }

    public int getM() {
        return m_;
    }

    public int getNNZ() {
        return xRow.size();
    }
}
