package com.petuum.app;

import com.petuum.ps.common.ClientTableConfig;
import com.petuum.ps.common.PSTableGroup;
import com.petuum.ps.common.TableGroupConfig;
import com.petuum.ps.common.client.ClientTable;
import com.petuum.ps.common.consistency.ConsistencyModel;
import com.petuum.ps.common.storage.DenseRow;
import com.petuum.ps.common.util.DenseMatrixLoader;
import jdk.nashorn.internal.ir.Block;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.BrokenBarrierException;

/**
 * Created by JunjieHu on 9/10/14.
 */
public class MetricLearn {
    private static Path hostFile = FileSystems.getDefault().getPath("machines", "localserver");
    private static Path trainFile = FileSystems.getDefault().getPath("dataset", "iris.txt");
   // private static Path testFile = FileSystems.getDefault().getPath("dataset", "synthetics100_0_of_10_test.dat");
    private static Path consFile = FileSystems.getDefault().getPath("dataset","C.txt");
    private static Path outputPrefix = FileSystems.getDefault().getPath("test");
    private static double lambda = 0.0;
    private static double initStepSize = 0.5;
    private static double stepSizeOffset = 100;
    private static double stepSizePow = 0.5;
    private static int rngSeed = 967234;
    private static int numClient = 1;
    private static int numWorkerThreads = 1;
    private static int numTotalWorker = 1;
    private static int clientID = 0;
    private static int K = 2;
    private static int numIterations = 54983;
    private static int staleness = 0;
    private static DenseMatrixLoader consMatrix;
    private static DenseMatrixLoader trainMatrix;
    private static DenseMatrixLoader testMatrix;
    private static Vector<MetricLearnBlock> blocks;

    // parameters for ITML
    private static int dimData;
    private static int numCons;
    private static float gammaITML = 1;
    private static float threshITML = 0.001f;
    private static int numBlockInRow = 2;

    private static int getTotalNumWorker() {
        return numTotalWorker;
    }
    private static int getGlobalWorkerId(int localThreadId) {
        // the last client has less threads
        if(clientID == (numClient-1)){
            return clientID * (numWorkerThreads+1) + localThreadId;
        }else{
            return clientID * numWorkerThreads + localThreadId;
        }
    }

    private static void printA(ClientTable tableA){
        for(int j = 0;j < dimData; j++){
            DenseRow r = (DenseRow)tableA.get(j);
            for(int t = 0; t < dimData; t++) {
                System.out.format("%.5f\t",r.get(t));
            }
            System.out.format("\n");
        }
        System.out.println("END of Print\n");
    }
    private static class MetricLearnBlock{
        MetricLearnBlock(int globalWorkerId){
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
        public boolean isDiagonal;
        public int blockRowIndexi;
        public int blockColumnIndexj;
        public int rowSize;
        public int columnSize;
        public int startRow;
        public int startColumn;
    }

    public static class SolveITML implements Runnable {
        public SolveITML(int localThreadId) {this.localThreadId = localThreadId; }
        public void run() {
            //register this thread with Petuum PS
            try {
                PSTableGroup.registerThread();
                //get table
                tableA = PSTableGroup.getTableOrDie(0);
                tableParam = PSTableGroup.getTableOrDie(1);
                tableDistance = PSTableGroup.getTableOrDie(2);

                //Initialize ITML solver
                int globalWorkerId = getGlobalWorkerId(localThreadId);

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
                    bpRow(globalWorkerId, iter);
                    pregetNextRow(globalWorkerId);
                    PSTableGroup.clock();
                    if(globalWorkerId == 0){
                        //printA(tableA);
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

        private static void initITML(int globalWorkerId){
            // initial A
            for(int i = 0; i < dimData; i++){
                Map<Integer,Double> updateA = new HashMap<Integer,Double>();
                for(int j = 0; j < dimData; j++){
                    if(i==j)
                        updateA.put(j,1.0);
                    else
                        updateA.put(j,0.0);
                }
                tableA.batchInc(i,updateA);
            }

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
                int idx = 0;
                for( int j = 0; j < getTotalNumWorker(); j++){
                    MetricLearnBlock block = blocks.get(j);
                    if(block.isDiagonal){
                        Double dist = 0.0;
                        for(int t = block.startRow; t < block.startRow +block.rowSize; t++){
                            dist += Math.pow(x1.get(t)-x2.get(t),2);
                        }
                        updateConsDistance.put(j, dist);
                    }else{
                        updateConsDistance.put(j, 0.0);
                    }
                }
                tableDistance.batchInc(i,updateConsDistance);   // tableDistance: numCons x getTotalNumWorker() matrix
            }
        }
        private static boolean bpRow(int globalWorkerId, int iter){
            DenseMatrixLoader.Row consRow = consMatrix.getNextRow(globalWorkerId);
            Vector<Float> row = consRow.value;
            int rowNum = consRow.rowNum;
            MetricLearnBlock globalBlock = blocks.get(globalWorkerId);
            float idx1 = row.get(0);
            float idx2 = row.get(1);
            int i1 = (int)idx1;
            int i2 = (int)idx2;
            DenseRow lambda    = (DenseRow)tableParam.get(0);
            DenseRow lambdaold = (DenseRow)tableParam.get(1);
            DenseRow bhat      = (DenseRow)tableParam.get(2);

            // v = (x_i1 - x_i2), O(dimData)
            Vector<Float> v = new Vector<Float>();
            for(int i = 0; i < dimData; i++)
                v.add(trainMatrix.getElement(i1,i)-trainMatrix.getElement(i2,i));

            // wtw = v'*A*v
            float wtw = 0.0f;
            DenseRow dis = (DenseRow) tableDistance.get(rowNum);
            for(int j = 0; j < getTotalNumWorker(); j++){
                wtw += dis.get(j);
            }

            if(Math.abs(bhat.get(rowNum)) < 10e-10){
                // output error to log
                System.out.println("bhat should never be 0!");
            }

            float gammaProj = (Float.POSITIVE_INFINITY== gammaITML)? 1: gammaITML / (gammaITML + 1);
            double alpha, beta, bhatNew;
            double lambdai = tableParam.get(0).get(rowNum);
            double bhati = tableParam.get(2).get(rowNum);
            if(Math.abs(row.get(2) - 1) < 10e-10){
                alpha = Math.min( lambdai, gammaProj * (1/wtw - 1/bhati));
                beta = alpha / (1 - alpha * wtw);
                bhatNew = 1 / ( (1/bhat.get(rowNum)) + (alpha/gammaITML) );
            }else{
                alpha = Math.min(lambdai, gammaProj * (1/bhati - 1/wtw));
                beta = -alpha / (1 + alpha*wtw);
                bhatNew = 1 / ( (1/bhat.get(rowNum)) - (alpha/gammaITML) );
            }
            tableParam.inc(0, rowNum, -alpha/getTotalNumWorker());  // lambda(rowNum) = lambda(rowNum) - alpha;
            tableParam.inc(2 ,rowNum, (bhatNew - tableParam.get(2).get(rowNum))/getTotalNumWorker());  // update bhat(rowNum)

            // A = A + (beta*A*v*v'*A);
            // t1 = (A * v) [ globalBlock.startRow : globalBlock.startRow + globalBlock.rowSize -1 ]
            // t2 = (A * v) [ globalBlock.startColumn : globalBlock.startColumn + globalBlock.columnSize -1 ]
            Vector<Float> t1 = new Vector<Float>();
            Vector<Float> t2 = new Vector<Float>();
            for( int i = 0; i < globalBlock.columnSize; i++){
                DenseRow ai = (DenseRow)tableA.get(i + globalBlock.startColumn);
                float aiv = 0.0f;
                for( int j = 0; j < dimData; j++){
                    aiv += ai.get(j)*v.get(j);
                }
                t2.add(i,aiv);
            }
            for( int i = 0; i < globalBlock.rowSize; i++) {
                DenseRow ai = (DenseRow) tableA.get(i + globalBlock.startRow);
                float aiv = 0.0f;
                for (int j = 0; j < dimData; j++) {
                    aiv += ai.get(j) * v.get(j);
                }
                t1.add(i, aiv);
            }
            // Update the corresponding globalBlock of A: A = A + beta t1 * t2
            for( int i = globalBlock.startRow; i < globalBlock.startRow + globalBlock.rowSize; i++){
                Map<Integer,Double> updateAi = new HashMap<Integer, Double>();
                for( int j = 0; j< globalBlock.columnSize; j++ ){
                    updateAi.put( j + globalBlock.startColumn, beta * t1.get(i - globalBlock.startRow) * t2.get(j));
                }
                tableA.batchInc(i,updateAi);
            }

            // test whether ITML converges
            double conv = 0.0f;
            boolean flag = false;
            if (consRow.isLastRow){
                double normsum = 0, norm1 = 0, norm2 = 0, norm3 = 0;
                HashMap<Integer,Double> updateLambdaOld = new HashMap<Integer, Double>();
                for(int i = 0; i < numCons; i++){
                    norm1 += Math.pow(lambda.get(i), 2);
                    norm2 += Math.pow(lambdaold.get(i),2);
                    norm3 += Math.abs(lambda.get(i) - lambdaold.get(i));
                    updateLambdaOld.put(i, (lambda.get(i) - lambdaold.get(i))/getTotalNumWorker());
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

            if(globalWorkerId == 0){
                System.out.format("%d-th thread: wtw = %f \n", globalWorkerId, wtw);
                System.out.format("beta = %.5f\n",beta);
                System.out.format("BP Row: %d-th row\n",consRow.rowNum);
                printA(tableA);
                if(iter==74) {
                    int a =1;
                }
            }

            return flag;
        }
        private static void pregetNextRow(int globalWorkerId){
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

            MetricLearnBlock globalBlock = blocks.get(globalWorkerId);
            // wtw = v' * A * v
            double dij = 0.0f, wtw = 0.0f;
            for( int i = 0; i < globalBlock.rowSize; i++){
                DenseRow ai = (DenseRow)tableA.get(i+globalBlock.startRow);
                float aiv = 0;
                for(int j = 0; j < globalBlock.columnSize; j++){
                    aiv += ai.get(j+globalBlock.startColumn) * v.get(j+globalBlock.startColumn);
                }
                dij += v.get(i) * aiv;
            }
            dij = globalBlock.isDiagonal? dij:(2*dij);
            tableDistance.inc(consRow.rowNum, globalWorkerId, dij-tableDistance.get(consRow.rowNum).get(globalWorkerId));
        }

        private int localThreadId;
        private MetricLearnBlock globalBlock;
        private static ClientTable tableA;
        private static ClientTable tableParam;
        private static ClientTable tableDistance;
    }

    public static void main(String[] args) throws Exception{
        //set the number of workers for each client
        numBlockInRow = 2;
        numTotalWorker = (numBlockInRow+1)*numBlockInRow/2;
        numWorkerThreads = numTotalWorker / numClient;
        numWorkerThreads = numTotalWorker % numClient ==0? numWorkerThreads: numWorkerThreads+1;
        if( clientID == (numClient-1)) {
            numWorkerThreads = numTotalWorker - (numClient - 1) * numWorkerThreads;
        }

        //configure Petuum PS
        TableGroupConfig tableGroupConfig = new TableGroupConfig();
        tableGroupConfig.numTotalServerThreads = numClient;
        tableGroupConfig.numTotalBgThreads = numClient;
        tableGroupConfig.numTotalClients = numClient;
        tableGroupConfig.numTables = 3;
        tableGroupConfig.getHostInfos(hostFile);
        tableGroupConfig.consistencyModel = ConsistencyModel.SSP;

        // local parameters for this process
        tableGroupConfig.numLocalServerThreads = 1;
        tableGroupConfig.numLocalBgThreads = 1;
        tableGroupConfig.numLocalAppThreads = numWorkerThreads + 1;
        tableGroupConfig.clientId = clientID;

        // register DenseRow<float> as 0.
        PSTableGroup.registerRow(0, DenseRow.class);
        PSTableGroup.init(tableGroupConfig, false);

        // load data
        trainMatrix = new DenseMatrixLoader(trainFile, getTotalNumWorker());
       // testMatrix = new DenseMatrixLoader(testFile, getTotalNumWorker());
        consMatrix = new DenseMatrixLoader(consFile,getTotalNumWorker());
        dimData = trainMatrix.getM(); //get the dimension of samples
        numCons = consMatrix.getN();  //get the number of constrains

        //config ps table
        ClientTableConfig tableConfig = new ClientTableConfig();
        tableConfig.tableInfo.rowType = 0; //dense row
        tableConfig.opLogCapacity = 300;
        tableConfig.tableInfo.tableStaleness = staleness;
        tableConfig.tableInfo.rowCapacity = dimData;  //dimension of the samples
        tableConfig.processCacheCapacity = 300;
        PSTableGroup.createTable(0, tableConfig);    //table for A
        tableConfig.tableInfo.rowCapacity = numCons;
        PSTableGroup.createTable(1,tableConfig);     //table for ITML parameters, e.g. lambda, lambdaold, bhat
        tableConfig.tableInfo.rowCapacity = getTotalNumWorker();
        PSTableGroup.createTable(2,tableConfig);     //table for distances of each pair in each constraint
        PSTableGroup.createTableDone();  //finished creating tables

        //get the configured list of blocks for all thread
        blocks = new Vector<MetricLearnBlock>();
        for(int i = 0; i < getTotalNumWorker(); i++){
            blocks.add(i, new MetricLearnBlock(i));
        }

        //run threads
        Vector<Thread> threads = new Vector<Thread>();
        for(int i=0;i<numWorkerThreads;i++){
            threads.add(new Thread(new SolveITML(i)));
            threads.get(i).start();
        }
        PSTableGroup.waitThreadRegister();

        //join
        for(int i = 0; i<numWorkerThreads; i++){
            threads.get(i).join();
        }

        //clean up
        PSTableGroup.shutDown();
    }
}
