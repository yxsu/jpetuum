package com.petuum.ps.server;

import static com.google.common.base.Preconditions.*;

import com.google.common.base.Preconditions;
import com.petuum.ps.common.Constants;
import com.petuum.ps.common.TableInfo;
import com.petuum.ps.common.util.BoolBox;
import com.petuum.ps.common.util.IntBox;
import com.petuum.ps.common.util.RecordBuff;
import com.petuum.ps.common.util.VectorClock;
import com.petuum.ps.oplog.SerializedOpLogReader;
import com.petuum.ps.thread.GlobalContext;
import com.petuum.ps.thread.ServerPushRowMsg;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
/**
 * Created by ZengJichuan on 2014/8/7.
 */
public class Server {
    private VectorClock clientClocks;
    private Map<Integer, VectorClock> clientVectorClockMap;
    private Map<Integer, Vector<Integer>> clientBgMap;
    private HashMap<Integer, ServerTable> tables;
    private static Logger log = LogManager.getLogger(Server.class);
    /**
     * mapping (clock, table id) to an array of read requests
     */
    private Map<Integer, Map<Integer, Vector<ServerRowRequest>>> clockBgRowReqeusts;

    private Vector<Integer> clinetIds;
    /**
     * latest oplog version that I have received from a bg thread
     */
    private Map<Integer, Integer> bgVersionMap;
    /**
     * Assume a single row does not exceed this size!
     */
    private static final int K_PUSH_ROW_MSG_SIZE_INIT = 4 *
            Constants.K_TOW_TO_POWER_TEN * Constants.K_NUM_BITS_PER_BYTE;

    private int pushRowMsgDataSize;

    private int serverId;

    public Server() {
        clientClocks = new VectorClock();
        clientVectorClockMap = new HashMap<Integer, VectorClock>();
        clientBgMap = new HashMap<Integer, Vector<Integer>>();
        tables = new HashMap<Integer, ServerTable>();
        clockBgRowReqeusts = new HashMap<Integer, Map<Integer, Vector<ServerRowRequest>>>();
        clinetIds = new Vector<Integer>();
        bgVersionMap = new HashMap<Integer, Integer>();
    }
    public void init(int serverId){
        for (Map.Entry<Integer, Vector<Integer>> entry : clientBgMap.entrySet()){
            VectorClock vectorClock = new VectorClock(entry.getValue());
            clientVectorClockMap.put(entry.getKey(), vectorClock);
            for (int bg : entry.getValue()){
                bgVersionMap.put(bg, -1);
            }
        }
        pushRowMsgDataSize = K_PUSH_ROW_MSG_SIZE_INIT;
        this.serverId = serverId;
    }
    public void addClientBgPair(int clientId, int bgId){
        if(clientBgMap.get(clientId) == null)
            clientBgMap.putIfAbsent(clientId, new Vector<Integer>());

        clientBgMap.get(clientId).add(bgId);
        clinetIds.add(clientId);
        clientClocks.addClock(clientId, 0);
    }
    public void CreateTable(int tableId, TableInfo tableInfo){
        tables.putIfAbsent(tableId, new ServerTable(tableInfo));//putIfAbsent

        if (GlobalContext.getResumeClock() > 0){
            tables.get(tableId).readSnapShot(GlobalContext.getResumeDir(),
                    serverId, tableId, GlobalContext.getResumeClock());
        }
    }
    public ServerRow findCreateRow(int tableId, int rowId){
        ServerTable serverTable = checkNotNull(tables.get(tableId));
        ServerRow serverRow = serverTable.findRow(rowId);
        if(serverRow != null){
            return serverRow;
        }
        serverRow = serverTable.createRow(rowId);
        return serverRow;
    }

    public boolean clock(int clientId, int bgId){
        int newClock = clientVectorClockMap.get(clientId).tick(bgId);
        if (newClock == 0)
            return false;
        newClock = clientClocks.tick(clientId);
        if(newClock != 0){
            if (GlobalContext.getSnapshotClock() <= 0
                    || newClock % GlobalContext.getSnapshotClock() != 0){
                return true;
            }
            for (Map.Entry<Integer, ServerTable> entry : tables.entrySet()){
                entry.getValue().takeSnapshot(GlobalContext.getSnapshotDir(),serverId,
                        entry.getKey(), newClock );
            }
            return true;
        }
        return false;
    }

    public void addRowRequest(int bgId, int tableId, int rowId, int clock){
        ServerRowRequest serverRowRequest = new ServerRowRequest();
        serverRowRequest.bgId  = bgId;
        serverRowRequest.tableId = tableId;
        serverRowRequest.rowId = rowId;
        serverRowRequest.clock = clock;

        if( clockBgRowReqeusts.containsKey(clock) == false){
            clockBgRowReqeusts.put(clock, new HashMap<Integer, Vector<ServerRowRequest>>());
        }
        if (clockBgRowReqeusts.get(clock).containsKey(bgId) == false){
            clockBgRowReqeusts.get(clock).put(bgId, new Vector<ServerRowRequest>());
        }
        clockBgRowReqeusts.get(clock).get(bgId).add(serverRowRequest);
    }

    public void getFulfilledRowRequests(Vector<ServerRowRequest> requests){
        int clock = clientClocks.getMinClock();
        requests.clear();
        Map<Integer, Vector<ServerRowRequest>> bgRowRequests = clockBgRowReqeusts.get(clock);
        if (bgRowRequests == null)  return;
        for(Map.Entry<Integer, Vector<ServerRowRequest>> entry : bgRowRequests.entrySet()){
            requests.addAll(entry.getValue());
        }
        clockBgRowReqeusts.remove(clock);
    }

    public void applyOpLog(ByteBuffer oplog, int bgThreadId, int version){
        Preconditions.checkArgument(bgVersionMap.get(bgThreadId) + 1 == version);
        bgVersionMap.put(bgThreadId, version);

        SerializedOpLogReader opLogReader = new SerializedOpLogReader(oplog);
        if(opLogReader.restart() == false)  return;

        IntBox tableId = new IntBox();
        IntBox rowId = new IntBox();
        Map<Integer, Double> updates;
        BoolBox startedNewTable = new BoolBox();

        updates = opLogReader.next(tableId, rowId, startedNewTable);        //construct map

        ServerTable serverTable = null;
        if(updates != null){
            serverTable = tables.get(tableId.intValue);
        }
        while (updates != null){
            boolean found = serverTable.applyRowOpLog(rowId.intValue, updates);
            if(found == false){
                serverTable.createRow(rowId.intValue);
                serverTable.applyRowOpLog(rowId.intValue, updates);
            }

            updates = opLogReader.next(tableId, rowId, startedNewTable);
            if (updates == null)    break;
            if(startedNewTable.boolValue == true){
                serverTable = tables.get(tableId.intValue);
            }
        }
        log.info("Read and Apply Update Done");
    }

    public int getMinClock(){
        return clientClocks.getMinClock();
    }

    public int getBgVersion(int bgThreadId){
        return bgVersionMap.get(bgThreadId);
    }

    public void createSendServerPushRowMsgs(Method pushMsgSend){
        int clientId = 0;
        HashMap<Integer, RecordBuff> buffs = new HashMap<Integer, RecordBuff>();
        HashMap<Integer, ServerPushRowMsg> msgMap = new HashMap<Integer, ServerPushRowMsg>();
        for (clientId = 0; clientId < GlobalContext.getNumClients(); clientId ++){
            int headBgId = GlobalContext.getHeadBgId(clientId);
            for (int i = 0; i < GlobalContext.getNumBgThreads(); i++){
                int bgId = headBgId + i;
                ServerPushRowMsg msg = new ServerPushRowMsg(pushRowMsgDataSize);
                msgMap.put(bgId, msg);
                buffs.put(bgId, new RecordBuff(msg.getData()));
            }
        }
        int numTableLeft = GlobalContext.getNumTables();
        for (Map.Entry<Integer, ServerTable> entry : tables.entrySet()){
            int tableId = entry.getKey();
            log.info("Serializing table " + tableId);
            ServerTable serverTable = entry.getValue();
            for (int i = 0; i < GlobalContext.getNumClients(); i++) {
                int headBgId = GlobalContext.getHeadBgId(i);
                // Set table id
                for (int j = 0; j < GlobalContext.getNumBgThreads(); j++) {
                    int bgId = headBgId + j;
                    RecordBuff recordBuff = buffs.get(bgId);
                    int tableIdPos = recordBuff.getMemPos();
                    if (tableIdPos == -1){
                        log.info("Not enough space for table id, send out to "+bgId);
                        try {
                            pushMsgSend.invoke(ServerThreads.class, new Object[]{bgId, msgMap.get(bgId), false});
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        } catch (InvocationTargetException e) {
                            e.printStackTrace();
                        }
                        msgMap.get(bgId).getData().clear();     //not actually set 0
                        recordBuff.resetOffset();
                        tableIdPos = recordBuff.getMemPos();
                    }
                    recordBuff.putTableId(tableIdPos, tableId);
                }
            }
            //ServerTable packs the data.
            serverTable.initAppendTableToBuffs();
            IntBox failedBgId = new IntBox();
            IntBox failedClientId = new IntBox();
            boolean packSuc = serverTable.appendTableToBuffs(0, buffs, failedBgId, failedClientId, false);

            while (packSuc == false){
                log.info("Not enough space for appending row, send out to " + failedBgId);
                RecordBuff recordBuff = buffs.get(failedBgId);
                int buffEndPos = recordBuff.getMemPos();
                if (buffEndPos != -1){
                    recordBuff.putTableId(buffEndPos, GlobalContext.getSerializedTableEnd());
                }
                try {
                    pushMsgSend.invoke(ServerThreads.class, new Object[]{failedBgId, msgMap.get(failedBgId), false});
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }

                msgMap.get(failedBgId).getData().clear();

                recordBuff.resetOffset();

                int tableIdPos = recordBuff.getMemPos();
                recordBuff.putTableId(tableIdPos, tableId);
                packSuc = serverTable.appendTableToBuffs(failedClientId.intValue, buffs, failedBgId, failedClientId, true);
            }
            numTableLeft --;
            if(numTableLeft > 0){
                for (int i = 0; i < GlobalContext.getNumClients(); i++){
                    int headBgId = GlobalContext.getHeadBgId(clientId);
                    // Set separator between tables
                    for (int j = 0; j < GlobalContext.getNumBgThreads(); j++) {
                        int bgId = headBgId + i;
                        RecordBuff recordBuff = buffs.get(bgId);
                        int tableSepPos = recordBuff.getMemPos();
                        if (tableSepPos == -1){
                            log.info("Not enough space for table separator, send out to " + bgId);
                            try {
                                pushMsgSend.invoke(ServerThreads.class, new Object[]{bgId, msgMap.get(bgId), false});
                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            } catch (InvocationTargetException e) {
                                e.printStackTrace();
                            }
                            msgMap.get(bgId).getData().clear();
                            recordBuff.resetOffset();
                        }else{
                            recordBuff.putTableId(tableSepPos, GlobalContext.getSerializedTableSeparator());
                        }
                    }
                }
            } else
                break;
        }

        for (clientId = 0; clientId < GlobalContext.getNumClients(); clientId ++){
            int headBgId = GlobalContext.getHeadBgId(clientId);
            // Set separator between tables
            for (int i = 0; i < GlobalContext.getNumBgThreads(); i++){
                int bgId = headBgId + i;
                RecordBuff recordBuff = buffs.get(bgId);
                int tableEndPos = recordBuff.getMemPos();
                if (tableEndPos == -1){
                    log.info("Not enough space for table end, send out to " + bgId);
                    try {
                        pushMsgSend.invoke(ServerThreads.class, new Object[]{bgId, msgMap.get(bgId), true});
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                    continue;
                }
                tableEndPos = GlobalContext.getSerializedTableEnd();
                msgMap.get(bgId).setAvaiSize(buffs.get(bgId).getMemUsedSize());
                try {
                    pushMsgSend.invoke(ServerThreads.class, new Object[]{bgId, msgMap.get(bgId), true});
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                msgMap.remove(bgId);
            }
        }
    }
}

class ServerRowRequest {
    public int bgId;
    public int tableId;
    public int rowId;
    public int clock;
}