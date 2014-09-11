package com.petuum.app;

import com.petuum.ps.common.ClientTableConfig;
import com.petuum.ps.common.PSTableGroup;
import com.petuum.ps.common.TableGroupConfig;
import com.petuum.ps.common.client.ClientTable;
import com.petuum.ps.common.consistency.ConsistencyModel;
import com.petuum.ps.common.storage.DenseRow;
import com.petuum.ps.common.util.MatrixLoader;
import com.petuum.ps.common.util.StandardMatrixLoader;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.BrokenBarrierException;

/**
 * Created by JunjieHu on 9/10/14.
 */
public class MetricLearn {
    private static Path hostFile = FileSystems.getDefault().getPath("machines", "localserver");
    private static Path dataFile = FileSystems.getDefault().getPath("dataset", "9x9_3blocks");
    private static Path outputPrefix = FileSystems.getDefault().getPath("test");
    private static double lambda = 0.0;
    private static double initStepSize = 0.5;
    private static double stepSizeOffset = 100;
    private static double stepSizePow = 0.5;
    private static int rngSeed = 967234;
    private static int numClient = 1;
    private static int numWorkerThreads = 1;
    private static int clientID = 0;
    private static int K = 2;
    private static int numIterations = 10;
    private static int staleness = 0;
    private static MatrixLoader dataMatrix;

    private static int getTotalNumWorker() {
        return numClient * numWorkerThreads;
    }
    private static int getGlobalWorderId(int localThreadId) {
        return clientID * numWorkerThreads + localThreadId;
    }



    public static class SolveDML implements Runnable {
        public SolveDML(int localThreadId) {this.localThreadId = localThreadId; }
        public void run() {

        }
        private int localThreadId;
    }

    public static void main(String[] args) throws Exception{
        //configure Petuum PS
        TableGroupConfig tableGroupConfig = new TableGroupConfig();
        tableGroupConfig.numTotalServerThreads = numClient;
        tableGroupConfig.numTotalBgThreads = numClient;
        tableGroupConfig.numTotalClients = numClient;
        tableGroupConfig.numTables = 1;
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
        dataMatrix = new StandardMatrixLoader(dataFile, getTotalNumWorker());



    }

}
