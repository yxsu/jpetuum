package com.petuum.ps.common.util;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Vector;
import java.util.function.Consumer;

/**
 * Created by jjhu on 9/14/14.
 */
public class DenseMatrixLoader implements MatrixLoader {
    public class Row{
        public Vector<Float> value;
        public int rowNum;
        public boolean isLastRow;
    }
    private int n_; //row
    private int m_; //column
    private Vector<Vector<Float>> matrix;

    private int numWorkers;
    private int[] workerNextLinePos;

    public DenseMatrixLoader(Path inputFile, int numWorkers) throws IOException{
        this.numWorkers = numWorkers;
        this.workerNextLinePos = new int[numWorkers];
        // Initialize workers to start of data
        for(int i = 0; i < numWorkers; i++) {
            workerNextLinePos[i] = 0;
        }
        //Load data
        readDenseMatrix(inputFile);
    }

    private void readDenseMatrix(Path inputFile) throws IOException {
        matrix.clear();
        n_=0;
        m_=0;
        Files.lines(inputFile, StandardCharsets.US_ASCII).forEach(new Consumer<String>() {
            public void accept(String s) {
                String[] temp = s.split("\t");
                Vector<Float> row = new Vector<Float>();
                for(String item:temp){
                    row.add(Float.valueOf(item));
                }
                matrix.add(row);
                m_ = temp.length > m_? temp.length:m_;
            }
        });
        n_ = matrix.size();
    }

    public int getN() {
        return n_;
    }

    public int getM() {
        return m_;
    }

    public Vector<Float> getRow(int i){
        return matrix.get(i);
    }

    public float getElement(int i, int j){
        return matrix.get(i).get(j);
    }

    public Row getNextRow(int workerId) {
        int rowId = workerNextLinePos[workerId];
        Row result = new Row();
        result.value = matrix.get(rowId);
        result.rowNum = rowId;

        //Advance to next line
        workerNextLinePos[workerId] += 1;
        result.isLastRow = false;
        if(workerNextLinePos[workerId] >= getN()){
            // return to the start of the data
            workerNextLinePos[workerId] = 0;
            result.isLastRow = true;
        }
        return result;
    }

    // pre-get next row without advancing forward
    public Row pregetNextRow(int workerId) {
        int rowId = workerNextLinePos[workerId];
        Row result = new Row();
        result.value = matrix.get(rowId);
        result.rowNum = rowId;
        return result;
    }

    // not sure -- JJ
    public int getNNZ() { return 0;}
    public Element getNextEl(int workerId){
        return null;
    }
}
