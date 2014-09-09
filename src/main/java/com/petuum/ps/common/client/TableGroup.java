package com.petuum.ps.common.client;
import com.google.common.base.Preconditions;
import com.petuum.ps.common.comm.CommBus;
import com.petuum.ps.common.util.IntBox;
import com.petuum.ps.common.util.VectorClockMT;
import com.petuum.ps.common.ClientTableConfig;
import com.petuum.ps.common.TableGroupConfig;
import com.petuum.ps.server.NameNodeThread;
import com.petuum.ps.server.ServerThreads;
import com.petuum.ps.thread.BgWorkers;
import com.petuum.ps.thread.GlobalContext;
import com.petuum.ps.thread.ThreadContext;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

/**
* @author Yuxin Su
* @version 1.0
* @created 19-??-2014 15:26:05
*/
public class TableGroup {

	/**
	 * Max staleness among all tables.
	 */
	private  int max_table_staleness_;
	private  AtomicInteger num_app_threads_registered_ = new AtomicInteger();
	private  Map<Integer, ClientTable> tables_ = new HashMap<Integer, ClientTable>();
	private  VectorClockMT vector_clock_;
    private Method clockInternal;
    private CyclicBarrier registerBarrier;

	/**
	 *
	 * @param tableGroupConfig
	 * @param tableAccess
	 */
	public TableGroup(final TableGroupConfig tableGroupConfig, boolean tableAccess, IntBox initThreadID) throws NoSuchMethodException, InterruptedException, BrokenBarrierException {
        GlobalContext.init(tableGroupConfig.numTotalServerThreads,
                tableGroupConfig.numLocalServerThreads,
                tableGroupConfig.numLocalAppThreads,
                tableAccess ? tableGroupConfig.numLocalAppThreads : tableGroupConfig.numLocalAppThreads - 1,
                tableGroupConfig.numLocalBgThreads,
                tableGroupConfig.numTotalBgThreads,
                tableGroupConfig.numTables,
                tableGroupConfig.numTotalClients,
                tableGroupConfig.serverIds,
                tableGroupConfig.hostMap,
                tableGroupConfig.clientId,
                tableGroupConfig.serverRingSize,
                tableGroupConfig.consistencyModel,
                tableGroupConfig.aggressiveClock);
        num_app_threads_registered_.set(1);
        this.vector_clock_ = new VectorClockMT();
        int localIDMin = GlobalContext.getThreadIdMin(tableGroupConfig.clientId);
        int localIDMax = GlobalContext.getThreadIdMax(tableGroupConfig.clientId);
        GlobalContext.commBus = new CommBus(localIDMin, localIDMax, 1);
        initThreadID.intValue = localIDMin + GlobalContext.K_INIT_THREAD_ID_OFFSET;
        CommBus.Config config = new CommBus.Config(initThreadID.intValue, CommBus.K_NONE, "");
        GlobalContext.commBus.threadRegister(config);

        if(GlobalContext.getNameNodeClientId() == tableGroupConfig.clientId) {
            NameNodeThread.init();
            ServerThreads.init(localIDMin + 1);
        } else {
            ServerThreads.init(localIDMin);
        }
        //TODO: for test
        BgWorkers.init(tables_);
        ThreadContext.registerThread(initThreadID.intValue);
        if(tableAccess) {
            vector_clock_.addClock(initThreadID.intValue, 0);
        }
        if(tableGroupConfig.aggressiveClock) {
            clockInternal = TableGroup.class.getMethod("clockAggressive");
        } else {
            clockInternal = TableGroup.class.getMethod("clockConservative");
        }
	}

	public void clock(){
        ThreadContext.clock();
        try {
            clockInternal.invoke(TableGroup.this);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
    }

	public void clockAggressive(){
        for (ClientTable clientTable : tables_.values()){
            clientTable.clock();
        }
        int clock = vector_clock_.tick(ThreadContext.getId());
        if (clock != 0){
            BgWorkers.clockAllTables();
        }else{
            BgWorkers.sendOpLogsAllTables();
        }
	}

	public void clockConservative(){
        for (ClientTable clientTable : tables_.values()){
            clientTable.clock();
        }
        int clock = vector_clock_.tick(ThreadContext.getId());
        if (clock != 0) {
            BgWorkers.clockAllTables();
        }
	}

	/**
	 *
	 * @param table_id
	 * @param table_config
	 */
	public boolean createTable(int table_id, final ClientTableConfig table_config){
        if(table_config.tableInfo.tableStaleness > max_table_staleness_) {
            max_table_staleness_ = table_config.tableInfo.tableStaleness;
        }

        boolean suc = BgWorkers.createTable(table_id, table_config);
        if(suc && (GlobalContext.getNumAppThreads() == GlobalContext.getNumTableThreads())) {
            tables_.get(table_id).registerThread();
        }
        return suc;
	}

	public void createTableDone() throws BrokenBarrierException, InterruptedException {

        BgWorkers.waitCreateTable();
        registerBarrier = new CyclicBarrier(GlobalContext.getNumTableThreads());

	}

	public void deregisterThread() throws InterruptedException {
        BgWorkers.threadDeregister();
        GlobalContext.commBus.threadDeregister();
	}

	/**
	 *
	 * @param table_id
	 */
	public ClientTable getTableOrDie(int table_id){
        return Preconditions.checkNotNull(tables_.get(table_id));
	}

	public void globalBarrier(){
        for (int i = 0; i < max_table_staleness_ + 1; i++) {
            clock();
        }
    }

	public int registerThread() throws BrokenBarrierException, InterruptedException {
        int appThreadIdOffset = num_app_threads_registered_.getAndIncrement();

        int threadId = GlobalContext.getLocalIdMin() + GlobalContext.K_INIT_THREAD_ID_OFFSET + appThreadIdOffset;

        CommBus.Config config = new CommBus.Config(threadId, CommBus.K_NONE, "");
        GlobalContext.commBus.threadRegister(config);

        ThreadContext.registerThread(threadId);

        BgWorkers.threadRegister();
        vector_clock_.addClock(threadId, 0);

        for(Map.Entry<Integer, ClientTable> table : tables_.entrySet()) {
            table.getValue().registerThread();
        }

        registerBarrier.await();
        return threadId;
	}

	public void waitThreadRegister() throws BrokenBarrierException, InterruptedException {

        if(GlobalContext.getNumTableThreads() == GlobalContext.getNumAppThreads()) {
            registerBarrier.await();
        }

	}

    public void shutDown() throws InterruptedException {
        BgWorkers.threadDeregister();
        ServerThreads.shutdown();
        if(GlobalContext.getClientId() == GlobalContext.getNameNodeClientId()) {
            NameNodeThread.shutDown();
        }
        BgWorkers.shutDown();
        GlobalContext.commBus.threadDeregister();
        GlobalContext.commBus.close();
    }
}