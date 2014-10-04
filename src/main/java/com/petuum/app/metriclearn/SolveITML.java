package com.petuum.app.metriclearn;

import com.petuum.ps.common.PSTableGroup;
import com.petuum.ps.common.client.ClientTable;
import com.petuum.ps.common.storage.DenseRow;
import com.petuum.ps.common.util.DenseMatrixLoader;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.BrokenBarrierException;

/**
 * Created by jjhu on 9/30/2014.
 */
public class SolveITML implements Runnable {
    private int localThreadId;
    private MetricLearnBlock globalBlock;
    private BlockIndex blockIndex;
    private ClientTable tableA;
    private ClientTable tableParam;
    private ClientTable tableDistance;
    private static int staleness;
    private static int dimData;
    private static int numCons;
    private static int numIterations;
    private static DenseMatrixLoader consMatrix;
    private static DenseMatrixLoader trainMatrix;
    private static Vector<MetricLearnBlock> blocks;
    private static float gammaITML = 1;
    private static float threshITML = 0.001f;
    private static int maxSizeBlock;
    private static int numBlockInRow;


    public SolveITML(int localThreadId, int staleness, int numIterations, Vector<MetricLearnBlock> blocks,
                     int numBlockInRow, DenseMatrixLoader trainMatrix, DenseMatrixLoader consMatrix){
        this.localThreadId = localThreadId;
        this.numBlockInRow = numBlockInRow;
        this.trainMatrix = trainMatrix;
        this.consMatrix = consMatrix;
        this.dimData = trainMatrix.getM();
        this.numCons = consMatrix.getN();
        this.staleness = staleness;
        this.numIterations = numIterations;
        this.blocks = blocks;
        this.maxSizeBlock = blocks.get(0).rowSize * blocks.get(0).columnSize;
    }
    public void run() {
        //register this thread with Petuum PS
        try {
            PSTableGroup.registerThread();
            //get table
            tableA = PSTableGroup.getTableOrDie(0);
            tableParam = PSTableGroup.getTableOrDie(1);
            tableDistance = PSTableGroup.getTableOrDie(2);

            //Initialize ITML solver
            int globalWorkerId = MetricLearn.getGlobalWorkerId(localThreadId);
            blockIndex = new BlockIndex(globalWorkerId, numBlockInRow);
            globalBlock = blocks.get(blockIndex.blockRowIndexi * numBlockInRow + blockIndex.blockColumnIndexj);

            if(globalWorkerId == 0){
                initITML(globalWorkerId);
                System.out.format("After initial of A:\n");
                printA(tableA);
            }

            // wait until the 0-th worker finishes initialization of PSTable
            PSTableGroup.globalBarrier();
            for(int i = 0; i < staleness; i++){
                PSTableGroup.clock();
            }
            long start = System.currentTimeMillis();
            //run ITML solver
            for(int iter = 0; iter < numIterations; iter++){
                if(globalWorkerId == 0){
                    System.out.format("The %d-th of %d iteration :\n",iter,numIterations);
                }
                // read the constrains and perform Bregman Projection
                DenseMatrixLoader.Row consRow = consMatrix.getNextRow(globalWorkerId);
                DenseRow lambda    = (DenseRow) tableParam.get(0);
                DenseRow lambdaold = (DenseRow) tableParam.get(1);
                DenseRow bhat      = (DenseRow) tableParam.get(2);

                /*
                if(globalWorkerId == 1) {
                    printA(tableA);
                    int temp = 0;
                    int temp1 = 1;
                }
                */
                //Step 1: Calculate d_A(x1,x2)
                float wtw = calculateDistance(consRow.rowNum);
                //Step 2: Calculate beta
                float beta = calculateBeta(wtw, consRow, lambda.get(consRow.rowNum), bhat.get(consRow.rowNum));
                //Step 3: Update metrix A
                updateMatrixA(globalWorkerId, beta, consRow.value);
                //Step 4: Check whether ITML converges
                checkConvergence(consRow.isLastRow, lambda, lambdaold);
                //Step 5: Pre-calculate the distance of next row
                pregetNextRow(globalWorkerId);

                if(globalWorkerId == 0){
                    //printBlock(0,1);
                    //System.out.format("This is the %d-th thread\n",globalWorkerId);
                    //printBlock(0,0);
                    int tem2 = 0;
                }

                PSTableGroup.clock();
                if(globalWorkerId == 1){
                    System.out.format("%d-th thread: wtw = %f \n", globalWorkerId, wtw);
                    System.out.format("beta = %.5f\n",beta);
                    System.out.format("BP Row: %d-th row\n",consRow.rowNum);
                    printA(tableA);
                }
            }
            long end = System.currentTimeMillis();
            if(globalWorkerId == 0){
                System.out.println("Total time is " + String.valueOf((end-start)/1000f));
            }
            // Let stale values finish propagating (performs staleness+1 clock()s)
            PSTableGroup.globalBarrier();
            PSTableGroup.deregisterThread();

        }catch (BrokenBarrierException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void initITML(int globalWorkerId){
        // initial A
        for(int i = 0; i < numBlockInRow; i++){
            Map<Integer, Double> updateA = new HashMap<Integer, Double>();
            MetricLearnBlock bii = blocks.get( i*numBlockInRow+i );
            for(int j = 0; j < bii.rowSize; j++){
                updateA.put( j * bii.rowSize + j, 1.0);
            }
            tableA.batchInc( bii.globalWorkerId , updateA);
        }
        printA(tableA);

        // initial parameters of ITML
        Map<Integer,Double> updateLambda = new HashMap<Integer, Double>();
        Map<Integer,Double> updateBhat = new HashMap<Integer, Double>();
        for(int i = 0;i < numCons; i++){
            // initial lambda to be 0
            updateLambda.put(i,0.0);
            float temp = consMatrix.getRow(i).get(3);
            updateBhat.put(i,(double)temp);
        }
        tableParam.batchInc(0, updateLambda);
        tableParam.batchInc(1, updateLambda);
        tableParam.batchInc(2, updateBhat);

        // initial distance of each pair in each constraints
        for(int i = 0; i < numCons; i++){
            Map<Integer,Double> updateConsDistance = new HashMap<Integer, Double>();
            DenseMatrixLoader.Row row = consMatrix.getNextRow(globalWorkerId);
            float idx1 = row.value.get(0);
            float idx2 = row.value.get(1);
            int i1 = (int)idx1;
            int i2 = (int)idx2;
            Vector<Float> x1 = trainMatrix.getRow(i1);
            Vector<Float> x2 = trainMatrix.getRow(i2);
            for(int n = 0; n < numBlockInRow; n++){
                MetricLearnBlock block = blocks.get( n * numBlockInRow + n);
                Double dist = 0.0;
                for(int t = block.startRow; t < block.startRow +block.rowSize; t++){
                    dist += Math.pow(x1.get(t)-x2.get(t),2);
                }
                updateConsDistance.put( block.globalWorkerId , dist);
            }
            tableDistance.batchInc(i,updateConsDistance);   // tableDistance: numCons x getTotalNumWorker() matrix
        }
    }

    private float calculateDistance(int rowNum){
        // wtw = v'*A*v
        float wtw = 0.0f;
        DenseRow dis = (DenseRow) tableDistance.get(rowNum);
        for(int j = 0; j < ITML.getTotalNumWorker(); j++){
            wtw += dis.get(j);
        }
        return wtw;
    }

    private float calculateBeta(float wtw, DenseMatrixLoader.Row consRow, double lambda, double bhat){
        int rowNum = consRow.rowNum;
        Vector<Float> row = consRow.value;
        if(Math.abs(bhat) < 10e-10){
            // output error to log
            System.out.println("bhat should never be 0!");
        }
        float gammaProj = (Float.POSITIVE_INFINITY== gammaITML)? 1: gammaITML / (gammaITML + 1);
        double alpha, beta, bhatNew;
        if(Math.abs(row.get(2) - 1) < 10e-10){
            alpha = Math.min( lambda, gammaProj * (1/wtw - 1/bhat));
            beta = alpha / (1 - alpha * wtw);
            bhatNew = 1 / ( (1/bhat) + (alpha/gammaITML) );
        }else{
            alpha = Math.min(lambda, gammaProj * (1/bhat - 1/wtw));
            beta = -alpha / (1 + alpha*wtw);
            bhatNew = 1 / ( (1/bhat) - (alpha/gammaITML) );
        }
        tableParam.inc(0, rowNum, -alpha / ITML.getTotalNumWorker());  // lambda(rowNum) = lambda(rowNum) - alpha;
        tableParam.inc(2, rowNum, (bhatNew - bhat) / ITML.getTotalNumWorker());  // update bhat(rowNum)

        return (float)beta;
    }

    private void updateMatrixA(int globalWorkerId, float beta, Vector<Float> row){
        float idx1 = row.get(0);
        float idx2 = row.get(1);
        int i1 = (int)idx1;
        int i2 = (int)idx2;
        // v = (x_i1 - x_i2), O(dimData)
        Vector<Float> v = new Vector<Float>();
        for(int i = 0; i < dimData; i++)
            v.add(trainMatrix.getElement(i1,i)-trainMatrix.getElement(i2,i));

        Vector<DenseRow> rowI = new Vector<DenseRow>();
        for(int i = 0; i < numBlockInRow; i++){
            int workerId = blocks.get( blockIndex.blockRowIndexi * numBlockInRow + i).globalWorkerId;
            //System.out.format("block(%d,%d) : workerID = %d, globalworker = %d\n",blockIndex.blockRowIndexi,i,workerId,globalWorkerId);
            DenseRow bi = (DenseRow) tableA.get(workerId);
            rowI.add(i,bi);
        }
        Vector<Double> t1 = new Vector<Double>();
        for(int i = 0; i < globalBlock.rowSize; i++) {
            int idx = 0;
            double dist = 0;
            for (int j = 0; j < numBlockInRow; j++) {
                DenseRow aij = rowI.get(j);
                MetricLearnBlock bij = blocks.get(blockIndex.blockRowIndexi * numBlockInRow + j);
                for (int n = 0; n < bij.columnSize; n++){
                    if(globalWorkerId == 1){
                        System.out.format("%f\t", aij.get( i * bij.stepRow + n * bij.stepColumn ));
                    }
                    dist += aij.get( i * bij.stepRow + n * bij.stepColumn ) * v.get(idx);
                    idx ++;
                }
                if(globalWorkerId == 1){
                    System.out.format("\n");
                }
            }
            t1.add(i,dist);
        }


        rowI = new Vector<DenseRow>();
        for(int i = 0; i < numBlockInRow; i++){
            //System.out.format("block(%d,%d) :  globalworker = %d\n",blockIndex.blockColumnIndexj,i,globalWorkerId);
            //System.out.format("index of block = %d\n",blockIndex.blockColumnIndexj * numBlockInRow + i);
            int workerId = blocks.get( blockIndex.blockColumnIndexj * numBlockInRow + i).globalWorkerId;
            DenseRow bi = (DenseRow) tableA.get(workerId);
            rowI.add(i,bi);
        }
        Vector<Double> t2 = new Vector<Double>();
        for(int i = 0; i < globalBlock.columnSize; i++) {
            int idx = 0;
            double dist = 0;
            for (int j = 0; j < numBlockInRow; j++) {
                DenseRow aij = rowI.get(j);
                MetricLearnBlock bij = blocks.get(blockIndex.blockColumnIndexj * numBlockInRow + j);
                for (int n = 0; n < bij.columnSize; n++){
                    dist += aij.get( i * bij.stepRow + n * bij.stepColumn ) * v.get(idx);
                    idx ++;
                }
            }
            t2.add(i,dist);
        }
        if(globalWorkerId == 1){
            int s = 1;
        }
        // Update the corresponding globalBlock of A: A = A + beta t1 * t2
        Map<Integer,Double> updateA = new HashMap<Integer, Double>();
        for( int i = 0; i < globalBlock.rowSize; i++){
            for( int j = 0; j< globalBlock.columnSize; j++ ){
                updateA.put( i * globalBlock.stepRow + j * globalBlock.stepColumn, beta * t1.get(i) * t2.get(j));
            }
        }
        tableA.batchInc(globalWorkerId, updateA);
        if(globalWorkerId == 1){
            printA(tableA);
            int a = 1;
        }
    }
    private boolean checkConvergence(boolean isLastRow, DenseRow lambda, DenseRow lambdaold){
        // test whether ITML converges
        double conv = 0.0f;
        boolean flag = false;
        if (isLastRow){
            double normsum = 0, norm1 = 0, norm2 = 0, norm3 = 0;
            HashMap<Integer,Double> updateLambdaOld = new HashMap<Integer, Double>();
            for(int i = 0; i < numCons; i++){
                norm1 += Math.pow(lambda.get(i), 2);
                norm2 += Math.pow(lambdaold.get(i),2);
                norm3 += Math.abs(lambda.get(i) - lambdaold.get(i));
                updateLambdaOld.put(i, (lambda.get(i) - lambdaold.get(i))/MetricLearn.getTotalNumWorker());
            }
            normsum = Math.sqrt(norm1)+Math.sqrt(norm2);
            System.out.format("norm sum = %f\n",normsum);
            if(normsum == 0){
                //break;
                flag = true;
            }else{
                conv = norm3 / normsum;
                if(conv < threshITML){
                    //break;
                    flag =  true;
                }
            }
            tableParam.batchInc(1,updateLambdaOld); // lambdaOld = lambda
        }
        return false;
    }

    private void pregetNextRow(int globalWorkerId){
        DenseMatrixLoader.Row consRow = consMatrix.pregetNextRow(globalWorkerId);
        float idx1 = consRow.value.get(0);
        float idx2 = consRow.value.get(1);
        int i1 = (int)idx1;
        int i2 = (int)idx2;

        //System.out.format("PreGetNextRow: %d-th row\n",consRow.rowNum);
        // v = (x_i1 - x_i2), O(dimData)
        Vector<Float> v = new Vector<Float>();
        for(int i = 0; i < dimData; i++)
            v.add(trainMatrix.getElement(i1,i)-trainMatrix.getElement(i2,i));

        //MetricLearnBlock globalBlock = blocks.get(globalWorkerId);
        // wtw = v' * A * v
        double dij = 0.0f, wtw = 0.0f;
        DenseRow aij = (DenseRow) tableA.get(globalBlock.globalWorkerId);
        for( int i = 0; i < globalBlock.rowSize; i++){
            float aiv = 0;
            for(int j = 0; j < globalBlock.columnSize; j++){
                aiv += aij.get( i * globalBlock.stepRow + j * globalBlock.stepColumn ) * v.get( j + globalBlock.startColumn );
            }
            dij += v.get( i + globalBlock.startRow ) * aiv;
        }
        dij = globalBlock.isDiagonal? dij:(2*dij);
        tableDistance.inc(consRow.rowNum, globalWorkerId, dij-tableDistance.get(consRow.rowNum).get(globalWorkerId));
    }

    public void printA(ClientTable tableA){
        for(int n = 0; n < numBlockInRow; n++){
            for(int m = 0; m < numBlockInRow; m++){
                MetricLearnBlock block = blocks.get( n * numBlockInRow + m);
                DenseRow aij = (DenseRow) tableA.get(block.globalWorkerId);

                for(int i = 0; i < block.rowSize; i++){
                    for(int j = 0; j < block.columnSize; j++){
                        System.out.format("%.5f\t",aij.get( i * block.stepRow + j * block.stepColumn));
                    }
                    System.out.format("\n");
                }
                System.out.format("END of Print Block(%d,%d) in Thread %d\n\n",block.blockRowIndexi,block.blockColumnIndexj, block.globalWorkerId);
            }
        }
    }
    public void printBlock(int blocki, int blockj){
        int workerId = MetricLearnBlock.blockToWorkerId(blocki, blockj);
        MetricLearnBlock block = blocks.get( blocki * numBlockInRow + blockj);
        DenseRow aij = (DenseRow) tableA.get(workerId);
        for(int i = 0; i < block.rowSize; i++){
            for(int j = 0; j < block.columnSize; j++){
                System.out.format("%.5f\t",aij.get( i * block.stepRow + j * block.stepColumn));
            }
            System.out.format("\n");
        }
        System.out.format("END of Print Block(%d,%d) in Thread %d\n\n",block.blockRowIndexi,block.blockColumnIndexj, workerId);
    }
}