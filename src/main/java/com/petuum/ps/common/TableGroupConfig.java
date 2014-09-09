package com.petuum.ps.common;

import com.petuum.ps.common.HostInfo;
import com.petuum.ps.common.consistency.ConsistencyModel;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.function.Consumer;

/**
 * Created by admin on 2014/8/13.
 */
public class TableGroupConfig {
    public String statsPath;
    // ================= Global Parameters ===================
    // Global parameters have to be the same across all processes.

    /**
     *  Total number of servers in the system.
     */
    public int numTotalServerThreads;
    /**
     * Total number of tables the PS will have. Each init thread must make
     * num_tables CreateTable() calls.
     */
    public int numTables;
    /**
     * Total number of clients in the system
     */
    public int numTotalClients;
    /**
     * Number of total background worker threads in the system
     */
    public int numTotalBgThreads;

    // ===================== Local Parameters ===================
    // Local parameters can differ between processes, but have to sum up to global
    // parameters.
    /**
     * Number of local server threads.
     */
    public int numLocalServerThreads;
    /**
     * Number of local applications threads, including init thread.
     */
    public int numLocalAppThreads;
    /**
     * Number of local background worker threads.
     */
    public int numLocalBgThreads;
    /**
     * IDs of all servers.
     */
    public int[] serverIds;
    /**
     * mapping server ID to host info.
     */
    public Map<Integer, HostInfo> hostMap = new HashMap<Integer, HostInfo>();
    /**
     * My client id.
     */
    public int clientId;
    /**
     * If set to true, oplog send is triggered on every Clock() call.
     * If set to false, oplog is only sent if the process clock (representing all
     * app threads) has advanced.
     * Aggressive clock may reduce memory footprint and improve the per-clock
     * convergence rate in the cost of performance.
     * Default is false (suggested).
     */
    public boolean aggressiveClock;

    public ConsistencyModel consistencyModel;

    public int aggressiveCpu;

    // In Async+pushing,
    public int serverRingSize;

    public int snapshotClock;

    public int resumeClock;

    public String snapshotDir;

    public String resumeDir;

    public String occPathPrefix;

    public void getHostInfos(Path hostFile) throws IOException {

        Files.lines(hostFile).forEachOrdered(new Consumer<String>() {
            public void accept(String s) {
                System.out.println(s);
                String[] temp = s.split(" ");
                int id = Integer.valueOf(temp[0]);
                hostMap.putIfAbsent(id, new HostInfo(id, temp[1], temp[2]));
            }
        });
        getServerIDsFromHostMap();
    }
    private void getServerIDsFromHostMap() {
        serverIds = new int[hostMap.size() - 1];
        int index = 0;
        for(Integer id : hostMap.keySet()) {
            if(id == 0)
                continue;
            serverIds[index]= id;
            index++;
        }
    }
}