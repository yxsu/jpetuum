package com.petuum.ps.thread;

import com.google.common.base.Preconditions;
import com.petuum.ps.common.HostInfo;
import com.petuum.ps.common.comm.CommBus;
import com.petuum.ps.common.consistency.ConsistencyModel;

import java.util.ArrayList;
import java.util.Map;
import java.util.Vector;

/**
 * Init must have "happens-before" relation with all other functions.
 * After Init(), accesses to all other functions are concurrent.
 * Created by zjc on 2014/8/8.
 */
public class GlobalContext {
    public static CommBus commBus;
    public static final int K_MAX_NUM_THREADS_PER_CLIENT = 1000;
    public static final int K_BG_THREAD_ID_START_OFFSET = 100;
    public static final int K_INIT_THREAD_ID_OFFSET = 200;

    private static int numServers = 1;
    private static int numLocalServerThreads = 1;
    private static int numAppThreads = 1;
    private static int numTableThreads = 1;
    private static int numBgThreads = 1;
    private static int numTotalBgThreads = 1;
    private static int numTables = 1;
    private static int numClients = 0;
    private static int lockPoolSize;

    private static Map<Integer, HostInfo> hostMap;
    private static int clientId = 0;
    private static int[] serverIds;
    private static int serverRingSize;

    private static ConsistencyModel consistencyModel;
    private static int localIdMin;
    private static boolean aggressiveCpu;

    private static int snapshotClock;
    private static String snapshotDir;
    private static int resumeClock;
    private static String resumeDir;

    public static int getThreadIdMin(int clientId){
        return clientId * K_MAX_NUM_THREADS_PER_CLIENT;
    }
    public static int getThreadIdMax(int clientId){
        return (clientId+1) * K_MAX_NUM_THREADS_PER_CLIENT;
    }
    public static int getNameNodeId(){ return 0; }
    public static int getNameNodeClientId(){ return 0; }
    public static int getHeadBgId(int threadId){
        return getThreadIdMin(clientId) + K_BG_THREAD_ID_START_OFFSET;
    }
    public static int threadId2ClientId(int threadId){
        return threadId / K_MAX_NUM_THREADS_PER_CLIENT;
    }
    public static int getSerializedTableSeparator(){  return -1;  }
    public static int getSerializedTableEnd(){ return -2; }

    public static void init(int numServers,
                            int numLocalServerThreads,
                            int numAppThreads,
                            int numTableThreads,
                            int numBgThreads,
                            int numTotalBgThreads,
                            int numTables,
                            int numClients,
                            int[] serverIds,
                            Map<Integer, HostInfo> hostMap,
                            int clientId,
                            int serverRingSize,
                            ConsistencyModel model,
                            boolean aggressiveCpu) {
        GlobalContext.numServers = numServers;
        GlobalContext.numLocalServerThreads = numLocalServerThreads;
        GlobalContext.numAppThreads = numAppThreads;
        GlobalContext.numTableThreads = numTableThreads;
        GlobalContext.numBgThreads = numBgThreads;
        GlobalContext.numTotalBgThreads = numTotalBgThreads;
        GlobalContext.numTables = numTables;
        GlobalContext.numClients = numClients;
        GlobalContext.serverIds = serverIds;
        GlobalContext.hostMap = hostMap;
        GlobalContext.clientId = clientId;
        GlobalContext.serverRingSize = serverRingSize;
        GlobalContext.consistencyModel = model;
        GlobalContext.aggressiveCpu = aggressiveCpu;

        commBus = new CommBus(getThreadIdMin(clientId), getThreadIdMax(clientId), 1);

    }

    public static int getNumServers() {
        return numServers;
    }

    public static int getNumLocalServerThreads() {
        return numLocalServerThreads;
    }

    public static int getNumAppThreads() {
        return numAppThreads;
    }

    public static int getNumTableThreads() {
        return numTableThreads;
    }

    public static int getNumBgThreads() {
        return numBgThreads;
    }

    public static int getNumTotalBgThreads() {
        return numTotalBgThreads;
    }

    public static int getNumTables() {
        return numTables;
    }

    public static int getNumClients() {
        return numClients;
    }

    public static HostInfo getHostInfo(int entityId){
        return Preconditions.checkNotNull(hostMap.get(entityId));
    }

    public static int getClientId() {
        return clientId;
    }

    public static int[] getServerIds() {
        return serverIds;
    }

    public static int getBgPartitionNum(int rowId){
        return rowId % numBgThreads;
    }

    public static int getRowPartitionServerId(int tableId, int rowId){
        int serverIdIdx = rowId % numServers;
        return serverIds[serverIdIdx];
    }

    public static int getServerRingSize(){
        return serverRingSize;
    }

    public static ConsistencyModel getConsistencyModel() {
        return consistencyModel;
    }

    public static int getLocalIdMin() {
        return localIdMin;
    }

    public static boolean isAggressiveCpu() {
        return aggressiveCpu;
    }

    public static int getLockPoolSize() {
        final int K_STRIPED_LOCK_EXPANSION_FACTOR = 20;
        return (numAppThreads + numBgThreads) * K_STRIPED_LOCK_EXPANSION_FACTOR;
    }

    public static int getLockPoolSize(int tableCapacity){
        final int K_STRIPED_LOCK_REDUCTION_FACTOR = 20;
        return (tableCapacity <= 2 * K_STRIPED_LOCK_REDUCTION_FACTOR)?
                tableCapacity : tableCapacity/K_STRIPED_LOCK_REDUCTION_FACTOR;
    }

    public static int getSnapshotClock(){
        return snapshotClock;
    }

    public static String getSnapshotDir() {
        return snapshotDir;
    }

    public static int getResumeClock() {
        return resumeClock;
    }

    public static String getResumeDir() {
        return resumeDir;
    }


}
