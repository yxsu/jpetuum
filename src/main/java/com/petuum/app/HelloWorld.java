package com.petuum.app;

import com.petuum.ps.common.ClientTableConfig;
import com.petuum.ps.common.PSTableGroup;
import com.petuum.ps.common.TableGroupConfig;
import com.petuum.ps.common.client.ClientRow;
import com.petuum.ps.common.client.ClientTable;
import com.petuum.ps.common.consistency.ConsistencyModel;
import com.petuum.ps.common.storage.DenseRow;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import static com.google.common.base.Preconditions.*;
/**
 * Created by ZengJichuan on 2014/9/3.
 */
public class HelloWorld {
    private static Logger log = LogManager.getLogger(HelloWorld.class);
    private static Path hostFile = FileSystems.getDefault().getPath("machines","localserver");

    private static int numTotalServerThread = 1;
    private static int numTotalClient = 1;
    private static int numTotalBgThread = 1;
    private static int numTable = 1;
    private static int numServerThreads = 1;
    private static int numAppThreads = 2;
    private static int numBgThreads = 1;

    private static int clientId = 0;
//    private static int K = 100;
    private static int numIterations = 10;
    private static int staleness = 0;
    private static int resumeClock = 0;

    public static void main(String[] args) throws Exception{

        //configure Petuum PS
        TableGroupConfig tableGroupconfig = new TableGroupConfig();
        tableGroupconfig.numTotalServerThreads = numTotalServerThread;
        tableGroupconfig.numTotalBgThreads = numTotalBgThread;
        tableGroupconfig.numTotalClients = numTotalClient;
        tableGroupconfig.numTables = numTable;
        tableGroupconfig.getHostInfos(hostFile);

        tableGroupconfig.consistencyModel = ConsistencyModel.SSP;
        //local parameters for this process
        tableGroupconfig.numLocalServerThreads = numServerThreads;
        tableGroupconfig.numLocalBgThreads = numBgThreads;
        tableGroupconfig.numLocalAppThreads = numAppThreads;
        tableGroupconfig.clientId = clientId;
        //need to register row type
        //register DenseRow<float> as 0.
        log.info("Hello world starts here");

        PSTableGroup.registerRow(20, DenseRow.class);

        PSTableGroup.init(tableGroupconfig, false);         //true will add the bgThread to clockVec

        log.info("Initialized TableGroup");

        //config ps table
        ClientTableConfig tableConfig = new ClientTableConfig();
        tableConfig.tableInfo.rowType = 20; //dense row
        tableConfig.opLogCapacity = 1000;   //useless
        tableConfig.tableInfo.tableStaleness = staleness;
        tableConfig.tableInfo.rowCapacity = 1000;
        tableConfig.processCacheCapacity = 1000;
        tableConfig.threadCacheCapacity = 1000;

        boolean suc = PSTableGroup.createTable(0, tableConfig);
        assert suc == true;

        //finished creating tables
        PSTableGroup.createTableDone();

        int numWorkerThreads = numAppThreads - 1;
        ThreadContext threadContext = new ThreadContext();
        threadContext.numClients = numTotalClient;
        threadContext.clientId = clientId;
        threadContext.numAppThreadsPerClient = numAppThreads;
        threadContext.appThreadIdOffset = numServerThreads + numBgThreads + 1;
        threadContext.numIterations = numIterations;
        threadContext.staleness = staleness;
        threadContext.numTables = numTable;
        threadContext.resumeClock = resumeClock;

        //run threads
        ExecutorService threadPool = Executors.newFixedThreadPool(numWorkerThreads);
        for (int i = 0; i < numWorkerThreads; i++) {
            threadPool.execute(new workThreadMain(threadContext));
        }

        PSTableGroup.waitThreadRegister();

        threadPool.shutdown();
        while(!threadPool.isTerminated()){}

        PSTableGroup.shutDown();
    }


}

class workThreadMain implements Runnable {
    private ThreadContext threadContext;
    private static Logger log = LogManager.getLogger(HelloWorld.class);
    public workThreadMain(ThreadContext threadContext) {
        this.threadContext = threadContext;
    }

    public void run() {
        try {
            int threadId = PSTableGroup.registerThread();
            log.info("Thread has registered, thread_id = " + threadId);
            PSTableGroup.globalBarrier();
            stalenessTest(threadContext, threadId);
            PSTableGroup.deregisterThread();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void stalenessTest(ThreadContext threadContext, int threadId) {
        log.info("numTables = " + threadContext.numTables);
        Vector<ClientTable> tables = new Vector<ClientTable>();
        for (int tableId = 0; tableId < threadContext.numTables; tableId++) {
            tables.add(PSTableGroup.getTableOrDie(tableId));
            assert tables.get(tableId).getRowType() == 20;
        }
        for (int iteration = 0; iteration < threadContext.numIterations; iteration++) {
            // Test 1: read and verify my cell is valid
            for (ClientTable table : tables){
                DenseRow row0 = (DenseRow)table.get(0);
                double val = row0.get(threadId);
                checkArgument(val == iteration + threadContext.resumeClock);

                DenseRow row1 = (DenseRow)table.get(threadId);
                val = row1.get(threadId);
                checkArgument(val == iteration + threadContext.resumeClock);
            }
            // Update my cell
            Map<Integer, Double> updateBatch = new HashMap<Integer, Double>();
            updateBatch.put(threadId, 1d);
            for (ClientTable table : tables){
                table.batchInc(0, updateBatch);
                log.info("Thread "+threadId+" iteration "+ iteration+" update row "+0+" columnId "+threadId);
                table.batchInc(threadId, updateBatch);
                log.info("Thread "+threadId+" iteration "+ iteration+" update row "+threadId+" columnId "+threadId);
            }
            log.info("Calling clock from thread " + threadId);
            PSTableGroup.clock();
        }
    }
}

class ThreadContext{
    int numClients;
    int clientId;
    int appThreadIdOffset;
    int numAppThreadsPerClient;
    int numIterations;
    int staleness;
    int numTables;
    int resumeClock;
}