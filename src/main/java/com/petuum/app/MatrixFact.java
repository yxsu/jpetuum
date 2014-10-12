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

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

/**
* Created by suyuxin on 14-8-23.
*/
public class MatrixFact {
    @Option(name="-hostFile", required = true, usage = "Set the name of Petuum PS server configuration file")
    private String serverFile;
    @Option(name="-dataset", required = true, usage = "Set the name of sparse matrix file")
    private String datasetFile;
    private Path outputPrefix = FileSystems.getDefault().getPath("test");
    private double lambda = 0.0;
    private double initStepSize = 0.5;
    private double stepSizeOffset = 100;
    private double stepSizePow = 0.5;
    private int rngSeed = 967234;
    @Option(name="-numClient", usage = "Set the number of total client")
    private int numClient = 1;
    @Option(name="-numWorkerThreads", usage = "Set the number of worker threads per client")
    private int numWorkerThreads = 1;
    @Option(name="-clientID", required = true, usage = "Set client ID of worker node")
    private int clientID = 0;
    @Option(name="-k", required = true, usage = "Set the dimension of latent variable")
    private int K = 2;
    @Option(name="-iter", required = true, usage = "Set the number of iteration")
    private int numIterations = 10;
    @Option(name="-s", aliases = "-staleness", required = true, usage = "Set the max length of staleness")
    private int staleness = 0;
    private MatrixLoader dataMatrix;

    private void sgdElement(int i , int j, float xij, double stepSize, int globalWorkerId,
                            ClientTable tableL, ClientTable tableR, ClientTable tableLoss) {
        //read L(i, :) and R(:, j) from Petuum PS
        DenseRow li = (DenseRow)tableL.get(i, 0, K);
        DenseRow rj = (DenseRow)tableR.get(j, 0, K);
        //compute L(i, : ) * R(:, j)
        float liRj = 0;
        for(int k = 0; k < K; k++) {
            liRj += li.get(k) * rj.get(k);

        }
        // Update the loss function (does not include L2 regularizer term)
        tableLoss.inc(0, globalWorkerId, Math.pow(xij - liRj, 2));
        // Now update L(i,:) and R(:,j) based on the loss function at X(i,j).
        // The non-regularized loss function at X(i,j) is ( X(i,j) - L(i,:)*R(:,j) )^2.
        //
        // The non-regularized gradient w.r.t. L(i,k) is -2*X(i,j)R(k,j) + 2*L(i,:)*R(:,j)*R(k,j).
        // The non-regularized gradient w.r.t. R(k,j) is -2*X(i,j)L(i,k) + 2*L(i,:)*R(:,j)*L(i,k).
        Map<Integer, Double> liUpdate = new HashMap<Integer, Double>();
        Map<Integer, Double> rjUpdate = new HashMap<Integer, Double>();
        for(int k = 0; k < K; k++) {
            double gradient = 0;
            //compute update for L(i,k)
            gradient = -2 * (xij - liRj) * rj.get(k) + lambda * 2 * li.get(k);
            liUpdate.put(k, -gradient * stepSize);
            //compute update for R(k, j)
            gradient = -2 * (xij - liRj) * li.get(k) + lambda * 2 * rj.get(k);
            rjUpdate.put(k, -gradient * stepSize);
        }
        //commit updates to Petuum PS
        tableL.batchInc(i, liUpdate);
        tableR.batchInc(j, rjUpdate);
    }
    private int getTotalNumWorker() {
        return numClient * numWorkerThreads;
    }

    private int getGlobalWorkerId(int localThreadId) {
        return clientID * numWorkerThreads + localThreadId;
    }

    private void initMF(ClientTable tableL, ClientTable tableR) {
        Random rand = new Random(rngSeed);
        // Add a random initialization in [-1,1)/num_workers to each element of L and R
        int numWorkers = getTotalNumWorker();
        for(int i = 0; i < dataMatrix.getN(); i++) {
            Map<Integer, Double> updatesL = new HashMap<Integer, Double>();
            for(int k = 0; k < K; k++) {
                updatesL.put(k, (rand.nextDouble() - 0.5) * 2 / numWorkers);
            }
            tableL.batchInc(i, updatesL);
        }

        for(int j = 0; j < dataMatrix.getM(); j++) {
            Map<Integer, Double> updatesR = new HashMap<Integer, Double>();
            for(int k = 0; k < K; k++) {
                updatesR.put(k, (rand.nextDouble() - 0.5) * 2 / numWorkers);
            }
            tableR.batchInc(j, updatesR);
        }
    }

    public class SolveMF implements Runnable {
        public SolveMF(int localThreadId) {
            this.localThreadId = localThreadId;
        }
        public void run() {
            //register this thread with Petuum PS
            try {
                PSTableGroup.registerThread();
                //get tables
                ClientTable tableL = PSTableGroup.getTableOrDie(0);
                ClientTable tableR = PSTableGroup.getTableOrDie(1);
                ClientTable tableLoss = PSTableGroup.getTableOrDie(2);
                // Initialize MF solver
                int totalNumWorkers = getTotalNumWorker();
                int globalWorkerId = getGlobalWorkerId(localThreadId);

                initMF(tableL, tableR);
                PSTableGroup.globalBarrier();
                long start = System.currentTimeMillis();
                //run mf solver
                for(int iter = 0; iter < numIterations; iter++) {
                    if(globalWorkerId == 0) {
                        System.out.println("Iteration " + String.valueOf(iter + 1) + "/" + String.valueOf(numIterations));
                    }
                    //clear loss function table
                    DenseRow lossRow = (DenseRow)tableLoss.get(0);
                    tableLoss.inc(0, globalWorkerId, - lossRow.get(globalWorkerId));
                    // Divide matrix elements across workers, and perform SGD
                    double stepSize = initStepSize * Math.pow(stepSizeOffset + iter, - stepSizePow);

                    MatrixLoader.Element ele = dataMatrix.getNextEl(globalWorkerId);
                    while(ele.isLastEl == false) {
                        sgdElement(ele.row, ele.col, ele.value, stepSize, globalWorkerId, tableL, tableR, tableLoss);
                        ele = dataMatrix.getNextEl(globalWorkerId);
                    }
                    //output loss function
                    if(globalWorkerId == 0) {
                        lossRow = (DenseRow)tableLoss.threadGet(0);
                        double loss = 0;
                        for(int t = 0; t < totalNumWorkers; t++) {
                            loss += lossRow.get(t);
                        }
                        System.out.println("loss function = " + String.valueOf(loss));
                    }
                    PSTableGroup.clock();
                }
                long end = System.currentTimeMillis();
                if(globalWorkerId == 0) {
                    System.out.println("Total time is " + String.valueOf((end - start) / 1000f));
                }
                // Let stale values finish propagating (performs staleness+1 clock()s)
                PSTableGroup.globalBarrier();
                PSTableGroup.deregisterThread();

            } catch (BrokenBarrierException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        private int localThreadId;
    }

    public static void main(String[] args) {
        MatrixFact runner = new MatrixFact();
        CmdLineParser parser = new CmdLineParser(runner);
        try {
            parser.parseArgument(args);
            runner.run();
        } catch(CmdLineException e) {
            parser.printUsage(System.out);
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void run()  throws Exception {
        //configure Petuum PS
        TableGroupConfig tableGroupconfig = new TableGroupConfig();
        tableGroupconfig.numTotalServerThreads = numClient;
        tableGroupconfig.numTotalBgThreads = numClient;
        tableGroupconfig.numTotalClients = numClient;
        tableGroupconfig.numTables = 3;//L_table, R_table, loss_table
        tableGroupconfig.getHostInfos(FileSystems.getDefault().getPath("machines", serverFile));
        tableGroupconfig.consistencyModel = ConsistencyModel.SSP;
        //local parameters for this process
        tableGroupconfig.numLocalServerThreads = 1;
        tableGroupconfig.numLocalBgThreads = 1;
        tableGroupconfig.numLocalAppThreads = numWorkerThreads + 1;
        tableGroupconfig.clientId = clientID;
        //need to register row type
        //register DenseRow<float> as 0.
        PSTableGroup.registerRow(0, DenseRow.class);
        //next..
        PSTableGroup.init(tableGroupconfig, false);
        //load data
        dataMatrix = new StandardMatrixLoader(FileSystems.getDefault().getPath("dataset", datasetFile), getTotalNumWorker());

        //config ps table
        ClientTableConfig tableConfig = new ClientTableConfig();
        tableConfig.tableInfo.rowType = 0; //dense row
        tableConfig.opLogCapacity = 100;
        tableConfig.tableInfo.tableStaleness = staleness;
        tableConfig.tableInfo.rowCapacity = K;
        tableConfig.processCacheCapacity = 100;
        PSTableGroup.createTable(0, tableConfig);
        PSTableGroup.createTable(1, tableConfig);
        tableConfig.tableInfo.rowCapacity = getTotalNumWorker();
        PSTableGroup.createTable(2, tableConfig);

        //finished creating tables
        PSTableGroup.createTableDone();

        //run threads
        Vector<Thread> threads = new Vector<Thread>();

        for(int i = 0; i < numWorkerThreads; i++) {
            threads.add(new Thread(new SolveMF(i)));
            threads.get(i).start();
        }

        PSTableGroup.waitThreadRegister();

        //join
        for(int i = 0; i < numWorkerThreads; i++) {
            threads.get(i).join();
        }
        //cleanup
        PSTableGroup.shutDown();
    }
}
