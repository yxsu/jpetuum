package com.petuum.app.metriclearn;

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
public class ITML {
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
    private static Vector<Integer> numWorkerThreads;
    private static int numTotalWorker = 1;
    private static int clientID = 0;
    private static int K = 2;
    private static int numIterations = 54983;
    private static int staleness = 0;
    private static DenseMatrixLoader consMatrix;
    private static DenseMatrixLoader trainMatrix;
    private static DenseMatrixLoader testMatrix;
    private static Vector<MetricLearnBlock> blocks;
    private static int maxSizeBlock;

    // parameters for ITML
    private static int dimData;
    private static int numCons;
    private static float gammaITML = 1;
    private static float threshITML = 0.001f;
    private static int numBlockInRow = 2;

    public static int getTotalNumWorker() {
        return numTotalWorker;
    }
    public static int getGlobalWorkerId(int localThreadId) {
        int globalWorkerId = 0;
        for(int i = 0; i < clientID; i++){
            globalWorkerId += numWorkerThreads.get(i);
        }
        return globalWorkerId + localThreadId;
    }

    public static void printA(ClientTable tableA){
        for(int j = 0;j < dimData; j++){
            DenseRow r = (DenseRow)tableA.get(j);
            for(int t = 0; t < dimData; t++) {
                System.out.format("%.5f\t",r.get(t));
            }
            System.out.format("\n");
        }
        System.out.println("END of Print\n");
    }

    public static void main(String[] args) throws Exception{
        //set the number of workers for each client
        numBlockInRow = 2;
        numTotalWorker = (numBlockInRow+1)*numBlockInRow/2;

        int temp = numTotalWorker / numClient;
        numWorkerThreads = new Vector<Integer>();
        for(int i = 0; i < numClient - 1 ; i++){
            numWorkerThreads.add(i, numTotalWorker % numClient == 0 ? temp : temp + 1);
        }
        numWorkerThreads.add(numClient-1, numTotalWorker - (numClient - 1) * temp);

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
        tableGroupConfig.numLocalAppThreads = numWorkerThreads.get(clientID) + 1;
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

        //get the configured list of blocks for all thread
        blocks = new Vector<MetricLearnBlock>();
        for(int i = 0; i < numBlockInRow; i++){
            for(int j = 0; j < numBlockInRow; j++){
                blocks.add( i*numBlockInRow+j, new MetricLearnBlock(i,j,dimData,numBlockInRow));
            }
        }
        maxSizeBlock = blocks.get(0).rowSize * blocks.get(0).columnSize;
        /*
        blocks = new Vector<MetricLearnBlock>();
        for(int i = 0; i < getTotalNumWorker(); i++){
            blocks.add(i, new MetricLearnBlock(i,numBlockInRow,dimData));
        }*/

        //config ps table
        ClientTableConfig tableConfig = new ClientTableConfig();
        tableConfig.tableInfo.rowType = 0; //dense row
        tableConfig.opLogCapacity = 300;
        tableConfig.tableInfo.tableStaleness = staleness;
        tableConfig.tableInfo.rowCapacity = maxSizeBlock;  //dimension of the samples
        tableConfig.processCacheCapacity = 300;
        PSTableGroup.createTable(0, tableConfig);    //table for A: numTotalWorker x maxSizeBlock matrix
        tableConfig.tableInfo.rowCapacity = numCons;
        PSTableGroup.createTable(1,tableConfig);     //table for ITML parameters, e.g. lambda, lambdaold, bhat
        tableConfig.tableInfo.rowCapacity = getTotalNumWorker();
        PSTableGroup.createTable(2,tableConfig);     //table for distances of each pair in each constraint
        PSTableGroup.createTableDone();  //finished creating tables

        //run threads
        Vector<Thread> threads = new Vector<Thread>();
        for(int i = 0; i < numWorkerThreads.get(clientID); i++){
            threads.add(new Thread(new SolveITML(i,staleness, numIterations, blocks, numBlockInRow, trainMatrix, consMatrix)));
            threads.get(i).start();
        }
        PSTableGroup.waitThreadRegister();

        //join
        for(int i = 0; i < numWorkerThreads.get(clientID); i++){
            threads.get(i).join();
        }

        //clean up
        PSTableGroup.shutDown();
    }
}
