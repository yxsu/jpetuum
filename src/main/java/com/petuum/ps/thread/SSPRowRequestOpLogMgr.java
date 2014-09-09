package com.petuum.ps.thread;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AtomicLongMap;
import com.petuum.ps.common.util.IntBox;

import java.util.*;

/**
* Created by ZengJichuan on 2014/8/20.
*/
public class SSPRowRequestOpLogMgr implements RowRequestOpLogMgr {
    public SSPRowRequestOpLogMgr() {
        this.versionRequestCntMap = AtomicLongMap.create();
        this.pendingRowRequest = new HashMap<Integer, Map<Integer, List<RowRequestInfo>>>();
        this.versionOpLogMap = new HashMap<Integer, BgOpLog>();
    }

    /**
     * map <table_id, row_id> to a list of requests
     * The list is in increasing order of clock
     */
    private Map<Integer, Map<Integer, List<RowRequestInfo>>> pendingRowRequest;
    /**
     * version -> (table_id, OpLogPartition)
     * The version number of a request means that all oplogs up to and including
     * this version have been applied to this row.
     * An OpLogPartition of version V is needed for requests sent before the oplog
     * is sent. This means requests of version V - 1, V - 2, ...
     */
    private Map<Integer, BgOpLog> versionOpLogMap;


    // used for OpLogIter
    private int opLogIterVersionNext;
    private int opLogIterVersionStart;
    private int opLogIterVersionEnd;

    private AtomicLongMap<Integer> versionRequestCntMap;

    public boolean addRowRequest(RowRequestInfo request, int tableId, int rowId) {
        int version = request.version;
        request.sent = true;
        {
            if(pendingRowRequest.containsKey(tableId) == false) {
                pendingRowRequest.putIfAbsent(tableId, new HashMap<Integer, List<RowRequestInfo>>());
            }
            if(pendingRowRequest.get(tableId).containsKey(rowId) == false){
                pendingRowRequest.get(tableId).putIfAbsent(rowId, new LinkedList<RowRequestInfo>());
            }
            List<RowRequestInfo> requestList = pendingRowRequest.get(tableId).get(rowId);
            boolean requestAdded = false;
            // Requests are sorted in increasing order of clock number.
            // When a request is to be inserted, start from the end as the requst's
            // clock is more likely to be larger.
            ListIterator<RowRequestInfo> iterator = requestList.listIterator(requestList.size());
            while(iterator.hasPrevious()){
                int clock = request.clock;
                int preInd = iterator.previousIndex();
                RowRequestInfo rowRequestInfo = iterator.previous();
                if(clock >= rowRequestInfo.clock){
                    request.sent = false;
                    requestList.add(preInd+1, request);
                    requestAdded = true;
                    break;
                }
            }
            if( ! requestAdded){
                requestList.add(0, request);
            }
        }
        versionRequestCntMap.incrementAndGet(version);
        return request.sent;
    }

    public int informReply(int tableId, int rowId, int clock, int currentVersion, Vector<Integer> appThreadIds) {
        appThreadIds.clear();

        List<RowRequestInfo> requestLst = pendingRowRequest.get(tableId).get(rowId);
        int clockToRequest = -1;
        while(requestLst.isEmpty() == false){
            RowRequestInfo request = requestLst.get(0);
            if (request.clock <= clock){
                //remove the request
                int requestVersion = request.version;
                appThreadIds.add(request.appThreadId);
                requestLst.remove(0);
                // decrement the version count
                versionRequestCntMap.decrementAndGet(requestVersion);
                Preconditions.checkArgument(versionRequestCntMap.get(requestVersion) >= 0);
                // if version count becomes 0, remove the count
                if(versionRequestCntMap.get(requestVersion) == 0){
                    versionRequestCntMap.remove(requestVersion);
                    cleanVersionOpLogs(requestVersion, currentVersion);
                }
            }else {
                if(request.sent == false){
                    clockToRequest = request.clock;
                    request.sent = true;
                    int requestVersion = request.version;
                    versionRequestCntMap.decrementAndGet(requestVersion);

                    request.version = currentVersion - 1;
                    versionRequestCntMap.incrementAndGet(request.version);
                    if(versionRequestCntMap.get(requestVersion) == 0){
                        versionRequestCntMap.remove(requestVersion);
                        cleanVersionOpLogs(requestVersion, currentVersion);
                    }
                }
                break;
            }
        }
        // if there's no request in that list, I can remove the empty list
        if (requestLst.isEmpty())
            pendingRowRequest.get(tableId).remove(rowId);
        return clockToRequest;
    }

    /**
     * All oplogs that are saved must be of an earlier version than current
     * version, while a request could be from the current version.
     * @param requestVersion
     * @param currentVersion
     */
    private void cleanVersionOpLogs(int requestVersion, int currentVersion) {
        // The first version to be removed is current version, which is not yet stored
        // So nothing to remove.
        if (requestVersion + 1 == currentVersion){
            return;
        }
        // First, make sure there's no request from a previous version.
        // We do that by checking if there's an OpLog of this version,
        // if there is one, it must be save for some older requests.
        if (versionRequestCntMap.containsKey(requestVersion)){
            return;
        }
        int versionToRemove = requestVersion;
        do {
            // No previous OpLog, can remove a later version of oplog.
            versionOpLogMap.remove(versionToRemove + 1);
            ++versionToRemove;
            // Figure out how many later versions of oplogs can be removed.
        }while((versionRequestCntMap.containsKey(versionToRemove) == false)
                && versionToRemove != currentVersion);
    }


    public BgOpLog getOpLog(int version) {
        return Preconditions.checkNotNull(versionOpLogMap.get(version));
    }

    public void informVersionInc() {

    }

    public void serverAcknowledgeVersion(int serverId, int version) {

    }

    public boolean addOpLog(int version, BgOpLog opLog) {
        Preconditions.checkArgument(versionRequestCntMap.containsKey(version) == false);
        if (versionRequestCntMap.size() > 0){
            versionOpLogMap.put(version, opLog);
            return true;
        }
        return false;
    }

    public BgOpLog opLogIterInit(int startVersion, int endVersion) {
        opLogIterVersionStart = startVersion;
        opLogIterVersionEnd = endVersion;
        opLogIterVersionNext = opLogIterVersionStart + 1;
        return getOpLog(startVersion);
    }

    public BgOpLog opLogIterNext(IntBox version) {
        if(opLogIterVersionNext > opLogIterVersionEnd)
            return null;
        version.intValue = opLogIterVersionNext;
        ++opLogIterVersionNext;
        return getOpLog(version.intValue);
    }
}

