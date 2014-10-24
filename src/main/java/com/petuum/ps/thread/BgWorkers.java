package com.petuum.ps.thread;

import com.google.common.base.Preconditions;
import com.petuum.ps.common.*;
import com.petuum.ps.common.client.ClientRow;
import com.petuum.ps.common.client.ClientTable;
import com.petuum.ps.common.client.SSPClientRow;
import com.petuum.ps.common.client.SerializedRowReader;
import com.petuum.ps.common.comm.CommBus;
import com.petuum.ps.common.consistency.ConsistencyModel;
import com.petuum.ps.common.oplog.RowOpLog;
import com.petuum.ps.common.util.*;
import com.petuum.ps.oplog.OpLogSerializer;
import com.petuum.ps.oplog.TableOpLog;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;


import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
* Created by zjc on 2014/8/14.
*/
public class BgWorkers {
    public static void shutDown() throws InterruptedException {
        for(int i = 0; i < GlobalContext.getNumBgThreads(); i++) {
            threads.get(i).join();
        }
    }

    private static class BgContext {


        private BgContext() {
            serverTableOpLogSizeMap = new HashMap<Integer, Map<Integer, Integer>>();
            serverOpLogMsgMap = new HashMap<Integer, ClientSendOpLogMsg>();
            serverOpLogMsgSizeMap = new HashMap<Integer, Integer>();
            tableServerOpLogSizeMap = new HashMap<Integer, Integer>();
            serverVectorClock = new VectorClock();
        }

        /**
         * version of the data, increment when a set of OpLogs
         * are sent out; may wrap around
         * More specifically, version denotes the version of the
         * OpLogs that haven't been sent out.
         */
        public int version;
        public RowRequestOpLogMgr rowRequestOpLogMgr;
        /**
         * initialized by BgThreadMain(), used in CreateSendOpLogs()
         * For server x, table y, the size of serialized OpLog is ...
         */
        public Map<Integer, Map<Integer, Integer>> serverTableOpLogSizeMap;
        /**
         * The OpLog msg to each server
         */
        public Map<Integer, ClientSendOpLogMsg> serverOpLogMsgMap;
        /**
         * map server id to oplog msg size
         */
        public Map<Integer, Integer> serverOpLogMsgSizeMap;
        /**
         * size of oplog per table, reused across multiple tables
         */
        public Map<Integer, Integer> tableServerOpLogSizeMap;
        /**
         * Data members needed for server push
         */
        public VectorClock serverVectorClock;
    }

    private static Method myCreateClientRow;
    private static Method getRowOpLog;
    /* Functions for SSPValue */

    //function pointer GetRowOpLogFunc

    private static Vector<Thread> threads = new Vector<Thread>();
    private static Vector<Integer> threadIds = new Vector<Integer>();
    private static Map<Integer, ClientTable> tables;
    private static int idStart;

    private static CyclicBarrier initBarrier;
    private static CyclicBarrier createTableBarrier;

    private static ThreadLocal<BgContext> bgContext = new ThreadLocal<BgContext>();
    private static CommBus commBus;
    //function pointers
    private static Method commBusRecvAny;
    private static Method commBusRecvTimeOutAny;
    private static Method commBusSendAny;
    private static Method commBusRecvAsyncAny;
    private static Method commBusRecvAnyWrapper;


    private static AtomicInteger systemClock = new AtomicInteger();
    private static VectorClockMT bgServerClock = new VectorClockMT();
    private static ReentrantLock systemClockLock;

    private static Condition systemClockCv;
    private static HashMap<Integer, HashMap<Integer, Boolean>> tableOpLogIndex;
    private static Logger log = LogManager.getLogger(BgWorkers.class);

    private static ByteBuffer commBusRecvAnyBusy(IntBox senderId){
        ByteBuffer receivedBuff = null;
        try {
            receivedBuff = (ByteBuffer) commBusRecvAsyncAny.invoke(commBus, new Object[]{senderId});
            while (receivedBuff == null){
                receivedBuff = (ByteBuffer) commBusRecvAsyncAny.invoke(commBus, new Object[]{senderId});
            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        return receivedBuff;
    }

    public static boolean createTable(int tableId, ClientTableConfig tableConfig) {
        TableInfo tableInfo = tableConfig.tableInfo;
        BgCreateTableMsg bgCreateTableMsg = new BgCreateTableMsg(null);
        bgCreateTableMsg.setTableId(tableId);
        bgCreateTableMsg.setStaleness(tableInfo.tableStaleness);
        bgCreateTableMsg.setRowType(tableInfo.rowType);
        bgCreateTableMsg.setRowCapacity(tableInfo.rowCapacity);
        bgCreateTableMsg.setProcessCacheCapacity(tableConfig.processCacheCapacity);
        bgCreateTableMsg.setThreadCacheCapacity(tableConfig.threadCacheCapacity);
        bgCreateTableMsg.setOplogCapacity(tableConfig.opLogCapacity);
        commBus.sendInproc(idStart, bgCreateTableMsg.getByteBuffer());
        //wait
        IntBox senderId = new IntBox();
        ByteBuffer buffer = commBus.recvInproc(senderId);
        assert new NumberedMsg(buffer).getMsgType() == NumberedMsg.K_CREATE_TABLE_REPLY;
        return true;
    }

    public static void threadDeregister() {
        AppThreadDeregMsg msg = new AppThreadDeregMsg(null);
        sendToAllLocalBgThreads(msg);
    }

    public static ClientRow createClientRow(int clock, Row rowData) {
        return new ClientRow(clock, rowData);
    }

    public static SSPClientRow createSSPClientRow(int clock, Row rowdData) {
        return new SSPClientRow(clock, rowdData);
    }

    public static void init(Map<Integer, ClientTable> rTables) throws NoSuchMethodException {
        tables = rTables;
        idStart = GlobalContext.getHeadBgId(GlobalContext.getClientId());
        commBus = GlobalContext.commBus;

        //condition variable
        systemClockLock = new ReentrantLock();
        systemClockCv = systemClockLock.newCondition();

        int myClientId = GlobalContext.getClientId();
        int myHeadBgId = GlobalContext.getHeadBgId(myClientId);
        for (int i = 0; i < GlobalContext.getNumBgThreads(); i++){
            bgServerClock.addClock(myHeadBgId + i, 0);
        }
        initBarrier = new CyclicBarrier(GlobalContext.getNumBgThreads() + 1);
        createTableBarrier = new CyclicBarrier(2);
        if (GlobalContext.getNumClients() == 1) {
            commBusRecvAny = CommBus.class.getMethod("recvInproc", IntBox.class);
            commBusRecvAsyncAny = commBus.getClass().getMethod("recvInprocAsync",
                    new Class[]{IntBox.class});
            commBusRecvTimeOutAny = commBus.getClass().getMethod("recvInprocTimeout",
                    new Class[]{IntBox.class, long.class});
            commBusSendAny = commBus.getClass().getMethod("sendInproc",
                    new Class[]{int.class, ByteBuffer.class});
        }else{
            commBusRecvAny = commBus.getClass().getMethod("recv",
                    new Class[]{IntBox.class});
            commBusRecvAsyncAny = commBus.getClass().getMethod("recvAsync",
                    new Class[]{IntBox.class});
            commBusRecvTimeOutAny = commBus.getClass().getMethod("recvTimeOut",
                    new Class[]{IntBox.class, long.class});
            commBusSendAny = commBus.getClass().getMethod("send",
                    new Class[]{int.class, ByteBuffer.class});
        }

        ConsistencyModel consistencyModel = GlobalContext.getConsistencyModel();
        try {
            switch (consistencyModel) {
                case SSP:
//                    bgThreadMain = BgWorkers.class.getMethod("sspBgThreadMain");
                    myCreateClientRow = BgWorkers.class.getMethod("createSSPClientRow",
                            new Class[]{int.class, Row.class});
                    getRowOpLog = BgWorkers.class.getMethod("SSPGetRowOpLog",
                            new Class[]{TableOpLog.class, int.class});//RowOpLog **row_oplog_ptr
                    break;
                case SSPPush:
//                    bgThreadMain = BgWorkers.class.getMethod("sspGbThreadMain");
                    myCreateClientRow = BgWorkers.class.getMethod("createClientRow",
                            new Class[]{int.class, Row.class});
                    getRowOpLog = BgWorkers.class.getMethod("SSPGetRowOpLog",
                            new Class[]{TableOpLog.class, int.class});
                    break;
            }
            if (GlobalContext.isAggressiveCpu()){
                commBusRecvAnyWrapper = BgWorkers.class.getMethod("commBusRecvAnyBusy",
                        new Class[]{IntBox.class});
            }else{
                commBusRecvAnyWrapper = BgWorkers.class.getMethod("commBusRecvAnySleep",
                        new Class[]{IntBox.class});
            }
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < GlobalContext.getNumBgThreads(); i++) {
            threadIds.add(i + idStart);
            threads.add(new Thread(new BgThread(i + idStart)));
            threads.get(i).start();
        }
        try {
            initBarrier.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }
        threadRegister();
    }

    public static void threadRegister() {
        for (int bgId : threadIds){
            connectToBg(bgId);
        }
    }

    public static void waitCreateTable() throws BrokenBarrierException, InterruptedException {
        createTableBarrier.await();
    }

    public static void clockAllTables(){
        sendToAllLocalBgThreads(new BgClockMsg(null));
    }

    public static void sendOpLogsAllTables(){
        sendToAllLocalBgThreads(new BgSendOpLogMsg(null));
    }

    private static void sendToAllLocalBgThreads(NumberedMsg msg){
        for (int bgId : threadIds){
            commBus.sendInproc(bgId, msg.getByteBuffer());
        }
    }

    public void getAsyncRowRequestReply(){
        IntBox senderId = new IntBox();
        Preconditions.checkArgument(new NumberedMsg(commBus.recvInproc(senderId)).getMsgType() == NumberedMsg.K_ROW_REQUEST_REPLY);
    }

    public int getSystemClock(){
        return systemClock.intValue();
    }

    public void waitSystemClock(int myClock){
        while(systemClock.intValue() < myClock){
            try {
                systemClockCv.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static boolean requestRow(int tableId, int rowId, int clock){
        RowRequestMsg requestRowMsg = new RowRequestMsg(null);
        requestRowMsg.setTableId(tableId);
        requestRowMsg.setRowId(rowId);
        requestRowMsg.setClock(clock);

        int bgId = GlobalContext.getBgPartitionNum(rowId) + idStart;
        commBus.sendInproc(bgId, requestRowMsg.getByteBuffer());
        IntBox sendId = new IntBox();
        ByteBuffer buffer = (ByteBuffer)commBus.recvInproc(sendId);
        int msgType = new NumberedMsg(buffer).getMsgType();
        Preconditions.checkArgument(msgType == NumberedMsg.K_ROW_REQUEST_REPLY);
        return  true;
    }

    public static boolean requestSubRow(int tableId, int rowId, int start, int offset, int clock) {
        RowRequestMsg requestRowMsg = new RowRequestMsg(null);
        requestRowMsg.setTableId(tableId);
        requestRowMsg.setRowId(rowId);
        requestRowMsg.setStart(start);
        requestRowMsg.setOffset(offset);
        requestRowMsg.setClock(clock);

        int bgId = GlobalContext.getBgPartitionNum(rowId) + idStart;
        commBus.sendInproc(bgId, requestRowMsg.getByteBuffer());
        IntBox sendId = new IntBox();
        ByteBuffer buffer = (ByteBuffer)commBus.recvInproc(sendId);
        int msgType = new NumberedMsg(buffer).getMsgType();
        Preconditions.checkArgument(msgType == NumberedMsg.K_ROW_REQUEST_REPLY);
        return  true;
    }

    public static void requestRowAsync(int tableId, int rowId, int clock){
        RowRequestMsg requestRowMsg = new RowRequestMsg(null);
        requestRowMsg.setTableId(tableId);
        requestRowMsg.setRowId(rowId);
        requestRowMsg.setClock(clock);

        int bgId = GlobalContext.getBgPartitionNum(rowId) + idStart;
        commBus.sendInproc(bgId, requestRowMsg.getByteBuffer());
    }

    public static void connectToBg(int bgId) {
        AppConnectMsg appConnectMsg = new AppConnectMsg(null);
        commBus.connectTo(bgId, appConnectMsg.getByteBuffer());
    }

    public static ByteBuffer commBusRecvAnySleep(IntBox senderId){
        ByteBuffer receivedBuff = null;
        try {
            receivedBuff = (ByteBuffer) commBusRecvAny.invoke(commBus, new Object[]{senderId});
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        return receivedBuff;
    }

   public static RowOpLog SSPGetRowOpLog(TableOpLog tableOpLog, int rowId){
        return tableOpLog.getEraseOpLog(rowId);
   }
    private static void handleClockMsg(boolean clockAdvanced){
        BgOpLog bgOpLog = getOpLogAndIndex();
        createOpLogMsgs(bgOpLog);
        Map<Integer, ClientSendOpLogMsg> serverOpLogMsgMap = bgContext.get().serverOpLogMsgMap;
        for (Map.Entry<Integer, ClientSendOpLogMsg> entry : serverOpLogMsgMap.entrySet()){
            entry.getValue().setIsClock(clockAdvanced);
            entry.getValue().setClientId(GlobalContext.getClientId());
            entry.getValue().setVersion(bgContext.get().version);
            int serverId = entry.getKey();

            // delete bgOpLog
            try {
                commBusSendAny.invoke(commBus, serverId, entry.getValue().getByteBuffer());
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
        }

        bgContext.get().rowRequestOpLogMgr.
                addOpLog(bgContext.get().version, bgOpLog);
        bgContext.get().version++;
        bgContext.get().rowRequestOpLogMgr.informVersionInc();

    }

    /**
     * construct serverOpLogMsgMap
     * @param bgOpLog
     */
    private static void createOpLogMsgs(BgOpLog bgOpLog) {
        Map<Integer, ClientSendOpLogMsg> serverOpLogMsgMap = bgContext.get().serverOpLogMsgMap;
        Map<Integer, Map<Integer, Integer>> serverTableOpLogSizeMap = bgContext.
                get().serverTableOpLogSizeMap;
        Map<Integer, Integer> serverOpLogMsgSizeMap = bgContext.get().serverOpLogMsgSizeMap;

        Map<Integer, Map<Integer, ByteBuffer>> tableServerMemMap =
                new HashMap<Integer, Map<Integer, ByteBuffer>>();
        //initialize ?
        for (Map.Entry<Integer, ClientTable> entry : tables.entrySet()){
            tableServerMemMap.put(entry.getKey(), new HashMap<Integer, ByteBuffer>());
        }

        for (Map.Entry<Integer, Map<Integer, Integer>> entryServer : serverTableOpLogSizeMap.entrySet()){
            int serverId = entryServer.getKey();
            OpLogSerializer opLogSerializer = new OpLogSerializer();
            serverOpLogMsgSizeMap.put(serverId, opLogSerializer.init(entryServer.getValue()));
            serverOpLogMsgMap.put(serverId, new ClientSendOpLogMsg(serverOpLogMsgSizeMap.get(serverId)));
            ByteBuffer mem = serverOpLogMsgMap.get(serverId).getData();
            opLogSerializer.assignMem(mem);
            //get the basic start buffer

            for (Map.Entry<Integer, ClientTable> entryTable : tables.entrySet()){
                int tableId = entryTable.getKey();
                int tablePos = opLogSerializer.getTablePos(tableId);
                //table id
                mem.putInt(tablePos, tableId);
                //table update size             Useless!!
                mem.putInt(tablePos + Integer.BYTES, entryTable.getValue().getSampleRow().getUpdateSize());
                //offset for table rows
                mem.position(tablePos + Integer.BYTES + Integer.BYTES);
                //slice from the position to the limit(default the end)
                tableServerMemMap.get(tableId).put(serverId, mem.slice());

            }
        }
        for (Map.Entry<Integer, ClientTable> entry : tables.entrySet()){
            int tableId = entry.getKey();
            BgOpLogPartition opLogPartition = bgOpLog.get(tableId);
            opLogPartition.serializedByServer(tableServerMemMap.get(tableId));
        }
    }

    /**
     * what is the function of index?
     * @return
     */
    private static BgOpLog getOpLogAndIndex(){
        int[] serverIds = GlobalContext.getServerIds();
        int localBgIndex = ThreadContext.getId() - idStart;
        // get thread-specific data structure to assist oplog message creation
        // those maps may contain legacy data from previous runs
        Map<Integer, Map<Integer, Integer>> serverTableOpLogSizeMap =
                bgContext.get().serverTableOpLogSizeMap;
        Map<Integer, Integer> tableNumBytesByServer = bgContext.get().tableServerOpLogSizeMap;

        BgOpLog bgOplog = new BgOpLog();
        for(Map.Entry<Integer, ClientTable> entryTable : tables.entrySet()){
            int tableId = entryTable.getKey();
            TableOpLog tableOpLog = entryTable.getValue().getOpLog();

            //Get OpLog index
            Map<Integer, Boolean> newTableOpLogIndex =
                    entryTable.getValue().getAndResetOpLogIndex(localBgIndex);
            int tableUpdateSize = entryTable.getValue().getSampleRow().getUpdateSize();
            BgOpLogPartition bgTableOplog = new BgOpLogPartition(tableId, tableUpdateSize);
            for (int serverId : serverIds){
                tableNumBytesByServer.put(serverId, Integer.BYTES);
            }

            for (Map.Entry<Integer, Boolean> entryIndex : newTableOpLogIndex.entrySet()){
                int rowId = entryIndex.getKey();
                RowOpLog rowOpLog = null;
                try {
                    rowOpLog = (RowOpLog) getRowOpLog.invoke(BgWorkers.class,
                            new Object[]{tableOpLog, rowId});
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                if (rowOpLog == null){
                    continue;
                }
                //null in java hashmap means not found, but also means the value may be null too. it is hard
                //to distinguish, it may be solved by using guava special null
//                if (found && (row_oplog == 0)) {
//                    table_oplog_index_[table_id][row_id] = true;
//                    VLOG(0) << "found && row_oplog == 0";
//                    continue;
//                }
                // update oplog message size
                int serverId = GlobalContext.getRowPartitionServerId(tableId, rowId);
                int updateSize = rowOpLog.getSerializedSize();
                // 1) row id
                // 2) number of updates in that row
                // 3) total size for serialized updates
                tableNumBytesByServer.put(serverId, tableNumBytesByServer.get(serverId)
                        + Integer.BYTES + Integer.BYTES + updateSize);
                bgTableOplog.insertOpLog(rowId, rowOpLog);
            }
            bgOplog.add(tableId, bgTableOplog);
            for (Map.Entry<Integer, Integer> entry : tableNumBytesByServer.entrySet()){
                serverTableOpLogSizeMap.get(entry.getKey()).put(tableId, entry.getValue());
            }
        }
        return bgOplog;
    }
    static class BgThread implements Runnable{
        private int myId;
        BgThread(int threadId) {
            myId = threadId;
        }

        public void run() {
//        STATS_REGISTER_THREAD(kBgThread);
            log.info("Bg Worker starts here, my_id = " + String.valueOf(myId));
            ThreadContext.registerThread(myId);

            initBgContext();

            initCommBus(myId);

            bgServerHandshake();

            try {
                initBarrier.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
            }
            IntBox numConnectedAppThreads = new IntBox(0);
            IntBox numDeregisteredAppThreads = new IntBox(0);
            IntBox numShutdownAckedServers = new IntBox(0);

            recvAppInitThreadConnection(numConnectedAppThreads);

            if(myId == idStart){
                handleCreateTables();
                try {
                    createTableBarrier.await();         //there are 2 createTable threads?
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (BrokenBarrierException e) {
                    e.printStackTrace();
                }
            }
            ByteBuffer msgBuf = null;
            IntBox senderId = new IntBox();
            int msgType;
            ByteBuffer msgMem;
            boolean destroyMem = false;
            while(true){
                try {
                    msgBuf = (ByteBuffer) commBusRecvAnyWrapper.invoke(BgWorkers.class,
                            new Object []{senderId});

                msgType = new NumberedMsg(msgBuf).getMsgType();

                switch (msgType){
                    case NumberedMsg.K_APP_CONNECT:
                    {
                        log.info("get a AppConnectMsg from " + String.valueOf(senderId.intValue));
                        numConnectedAppThreads.intValue++;
                        Preconditions.checkArgument(
                                numConnectedAppThreads.intValue<=GlobalContext.getNumAppThreads());
                    }
                    break;
                    case NumberedMsg.K_APP_THREAD_DEREG:
                    {
                        log.info("get a AppThreadDeregMsg from " + String.valueOf(senderId.intValue));
                        numDeregisteredAppThreads.intValue++;
                        if (numDeregisteredAppThreads.intValue == GlobalContext.getNumAppThreads()){
                            try {
                                ClientShutDownMsg msg = new ClientShutDownMsg(null);
                                int nameNodeId = GlobalContext.getNameNodeId();
                                commBusSendAny.invoke(commBus,
                                        new Object[]{nameNodeId, msg.getByteBuffer()});
                                int numServers = GlobalContext.getNumServers();
                                int[] serverIds = GlobalContext.getServerIds();
                                for(int i = 0 ; i < numServers; i++){
                                    commBusSendAny.invoke(commBus,
                                            new Object[]{serverIds[i], msg.getByteBuffer()});
                                }
                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            } catch (InvocationTargetException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    break;
                    case NumberedMsg.K_SERVER_SHUT_DOWN_ACK:
                    {
                        log.info("get a ServerShutDownAck from " + String.valueOf(senderId.intValue));
                        numShutdownAckedServers.intValue++;
                        if(numShutdownAckedServers.intValue == GlobalContext.getNumServers() + 1){
                            commBus.threadDeregister();
                            shutdownClean();
                            return;
                        }
                    }
                    break;
                    case NumberedMsg.K_ROW_REQUEST:
                    {
                        log.info("get a RowRequestMsg from " + String.valueOf(senderId.intValue));
                        checkForwardRowRequestToServer(senderId.intValue, new RowRequestMsg(msgBuf));
                    }
                    break;
                    case NumberedMsg.K_SERVER_ROW_REQUEST_REPLY:
                    {
                        log.info("get a ServerRowRequestReply from " + String.valueOf(senderId.intValue));
                        handleServerRowRequestReply(senderId, new ServerRowRequestReplyMsg(msgBuf));
                    }
                    break;
                    case NumberedMsg.K_BG_CLOCK:
                    {
                        log.info("get a BgClockMsg from " + String.valueOf(senderId.intValue));
                        handleClockMsg(true);
                        //STATS_BG_CLOCK();
                    }
                    break;
                    case NumberedMsg.K_BG_SEND_OP_LOG:
                    {
                        log.info("get a BgSendOpLogMsg from " + String.valueOf(senderId.intValue));
                        handleClockMsg(false);
                    }
                    break;
                    case NumberedMsg.K_SERVER_PUSH_ROW:
                    {
                        log.info("get a ServerPushRow from " + String.valueOf(senderId.intValue));
                        ServerPushRowMsg serverPushRowMsg = new ServerPushRowMsg(msgBuf);
                        int version = serverPushRowMsg.getVersion();
                        bgContext.get().rowRequestOpLogMgr.serverAcknowledgeVersion(senderId.intValue, version);
                        applyServerPushedRow(version, serverPushRowMsg.getData());
//                        STATS_BG_ADD_PER_CLOCK_SERVER_PUSH_ROW_SIZE(
//                                server_push_row_msg.get_size());
                        boolean isClock = serverPushRowMsg.getIsClock();
                        if (isClock){
                            int serverClock = serverPushRowMsg.getClock();
                            Preconditions.checkArgument(
                                    bgContext.get().serverVectorClock.getClock(senderId.intValue)+1 == serverClock);
                            int newClock = bgContext.get().serverVectorClock.tick(senderId.intValue);
                            if (newClock != 0){
                                int newSystemClock = bgServerClock.tick(myId);
                                if (newSystemClock != 0){
                                    systemClock.incrementAndGet();
//                                    system_clock_cv_.notify_all();            //condition variable
                                    systemClockCv.signalAll();
                                }
                            }
                        }
                    }
                    break;
                    default:
                        log.error("Unrecognized Message from " + String.valueOf(senderId.intValue));
                }
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        private static void applyServerPushedRow(int version, ByteBuffer mem){
            //Row Reader
            SerializedRowReader rowReader = new SerializedRowReader(mem);
            if(!rowReader.restart())
                return ;
            IntBox tableId = new IntBox();
            IntBox rowId = new IntBox();
            ByteBuffer data = rowReader.next(tableId, rowId);

            int currentTableId = -1;
            int rowType = 0;
            ClientTable clientTable = null;
            while(data != null){
                if(currentTableId != tableId.intValue) {
                    Preconditions.checkNotNull(clientTable = tables.get(tableId));
                    rowType = clientTable.getRowType();
                    currentTableId = tableId.intValue;
                }
//                Row rowData = ClassRegistry<Row>.getRegistry().createObject(rowType);
                Row rowData = (Row) SerializationUtils.deserialize(data.array());
                applyOpLogsAndInsertRow(tableId.intValue, clientTable, rowId.intValue, version,
                        rowData, 0);
                data = rowReader.next(tableId, rowId);
            }
        }

        private static void handleServerRowRequestReply(IntBox serverId, ServerRowRequestReplyMsg serverRowRequestReplyMsg) throws InvocationTargetException, IllegalAccessException {
            int tableId = serverRowRequestReplyMsg.getTableId();
            int rowId = serverRowRequestReplyMsg.getRowId();
            int clock = serverRowRequestReplyMsg.getClock();
            int version = serverRowRequestReplyMsg.getVersion();

            ClientTable clientTable = tables.get(tableId);
            int rowType = clientTable.getRowType();
//            Row rowData = ClassRegister<Row>.getRegistry().createObject(rowType);
            Row rowData = (Row) SerializationUtils.deserialize(serverRowRequestReplyMsg.getRowData().array());
            bgContext.get().rowRequestOpLogMgr.serverAcknowledgeVersion(serverId.intValue, version);
            applyOpLogsAndInsertRow(tableId, clientTable, rowId, version, rowData, clock);

            Vector<Integer> appThreadIds = new Vector<Integer>();
            int clockToRequest = bgContext.get().rowRequestOpLogMgr.informReply(tableId, rowId, clock,
                    bgContext.get().version, appThreadIds);
            if (clockToRequest >= 0){
                RowRequestMsg rowRequestMsg = new RowRequestMsg(null);
                rowRequestMsg.setTableId(tableId);
                rowRequestMsg.setRowId(rowId);
                rowRequestMsg.setClock(clockToRequest);
                int serverIdNew = GlobalContext.getRowPartitionServerId(tableId, rowId);
                int sentSize = (Integer)commBusSendAny.invoke(commBus,
                        new Object[]{serverIdNew, rowRequestMsg.getByteBuffer()});
            }
            RowRequestReplyMsg rowRequestReplyMsg = new RowRequestReplyMsg(null);
            for (int appThreadId : appThreadIds){
                commBus.sendInproc(appThreadId, rowRequestReplyMsg.getByteBuffer());
            }
        }

        private static void applyOpLogsAndInsertRow(int tableId, ClientTable clientTable,
                                                    int rowId, int rowVersion, Row rowData, int clock) {
            applyOldOpLogsToRowData(tableId, clientTable, rowId, rowVersion, rowData);
            ClientRow clientRow = null;
            try {
                clientRow = (ClientRow) myCreateClientRow.invoke(BgWorkers.class, new Object[]{clock, rowData});
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
            TableOpLog tableOpLog = clientTable.getOpLog();
            RowOpLog rowOpLog = tableOpLog.findOpLog(rowId);
            if (rowOpLog != null){
                IntBox columnId = new IntBox();
                Double update = rowOpLog.beginIterate(columnId);
                while (update != null){
                    rowData.applyIncUnsafe(columnId.intValue, update);
                    update = rowOpLog.next(columnId);
                }
            }
            clientTable.insert(rowId, clientRow);
        }

        private static void applyOldOpLogsToRowData(int tableId, ClientTable clientTable, int rowId,
                                                    int rowVersion, Row rowData) {
            if(rowVersion + 1 < bgContext.get().version){
                BgOpLog bgOpLog = bgContext.get().rowRequestOpLogMgr.
                        opLogIterInit(rowVersion + 1, bgContext.get().version - 1);
                IntBox opLogVersion = new IntBox(rowVersion + 1);
                while (bgOpLog != null){
                    BgOpLogPartition bgOpLogPartition = bgOpLog.get(tableId);
                    // OpLogs that are after (exclusively) version should be applied
                    RowOpLog rowOpLog = bgOpLogPartition.findOpLog(rowId);
                    if(rowOpLog != null){
                        IntBox columnId = new IntBox();
                        Double update = rowOpLog.beginIterate(columnId);
                        while(update != null){
                            rowData.applyInc(columnId.intValue, update);
                            update = rowOpLog.next(columnId);
                        }
                    }
                    bgOpLog = bgContext.get().rowRequestOpLogMgr.opLogIterNext(opLogVersion);
                }
            }
        }

        private static void checkForwardRowRequestToServer(int appThreadId, RowRequestMsg rowRequestMsg) throws InvocationTargetException, IllegalAccessException {
            int tableId = rowRequestMsg.getTableId();
            int rowId = rowRequestMsg.getRowId();
            int clock = rowRequestMsg.getClock();

            // Check if the row exists in process cache
            ClientTable table = tables.get(tableId);
            ClientRow clientRow = table.getLocally(rowId);
            if (clientRow != null){
                if(clientRow.getClock() >= clock){
                    RowRequestReplyMsg rowRequestReplyMsg = new RowRequestReplyMsg(null);
                    commBus.sendInproc(appThreadId, rowRequestReplyMsg.getByteBuffer());
                    return;
                }
            }

            RowRequestInfo rowRequest = new RowRequestInfo();
            rowRequest.appThreadId = appThreadId;
            rowRequest.clock = rowRequestMsg.getClock();
            // Version in request denotes the update version that the row on server can
            // see. Which should be 1 less than the current version number.
            rowRequest.version = bgContext.get().version - 1;
            boolean shouldBeSent = bgContext.get().rowRequestOpLogMgr.
                    addRowRequest(rowRequest, tableId, rowId);
            if(shouldBeSent){
                int serverId = GlobalContext.getRowPartitionServerId(tableId, rowId);
                commBusSendAny.invoke(commBus, serverId, rowRequestMsg.getByteBuffer());
            }
        }

        private static void shutdownClean() {
            //delete bg_context_->row_request_oplog_mgr;
        }

        private static void handleCreateTables() {
            for (int numCreatedTables = 0; numCreatedTables < GlobalContext.getNumTables();
                 numCreatedTables++) {
                IntBox senderId = new IntBox();
                ClientTableConfig clientTableConfig = new ClientTableConfig();

                ByteBuffer msgBuffer = null;
                msgBuffer = commBus.recvInproc(senderId);
                int msgType = new NumberedMsg(msgBuffer).getMsgType();
                Preconditions.checkArgument(msgType == NumberedMsg.K_BG_CREATE_TABLE);
                BgCreateTableMsg bgCreateTableMsg = new BgCreateTableMsg(msgBuffer);
                //set up client table config
                clientTableConfig.tableInfo.tableStaleness = bgCreateTableMsg.getStaleness();
                clientTableConfig.tableInfo.rowType = bgCreateTableMsg.getRowType();
                clientTableConfig.tableInfo.rowCapacity = bgCreateTableMsg.getRowCapacity();
                clientTableConfig.processCacheCapacity =
                        bgCreateTableMsg.getProcessCacheCapacity();
                clientTableConfig.threadCacheCapacity =
                        bgCreateTableMsg.getThreadCacheCapacity();
                clientTableConfig.opLogCapacity = bgCreateTableMsg.getOplogCapacity();

                CreateTableMsg createTableMsg = new CreateTableMsg(null);
                createTableMsg.setTableId(bgCreateTableMsg.getTableId());
                createTableMsg.setStaleness(bgCreateTableMsg.getStaleness());
                createTableMsg.setRowType(bgCreateTableMsg.getRowType());
                createTableMsg.setRowCapacity(bgCreateTableMsg.getRowCapacity());
                int tableId = createTableMsg.getTableId();

                //send msg to name node
                try {
                    commBusSendAny.invoke(commBus, GlobalContext.getNameNodeId(), createTableMsg.getByteBuffer());
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }

                //wait for response from name node

                ByteBuffer msgBuf = null;
                IntBox nameNodeId = new IntBox();

                while(true) {
                    try {
                        msgBuf = (ByteBuffer) commBusRecvAny.invoke(commBus, nameNodeId);

                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                    msgType = new NumberedMsg(msgBuf).getMsgType();
                    log.info("receive a message with type " + String.valueOf(msgType) + " from " + String.valueOf(nameNodeId.intValue));
                    if(msgType == NumberedMsg.K_CREATE_TABLE_REPLY)break;
                }

                CreateTableReplyMsg createTableReplyMsg = new CreateTableReplyMsg(msgBuf);
                Preconditions.checkArgument(createTableReplyMsg.getTableId() == tableId);

                //Create ClientTable
                ClientTable clientTable = new ClientTable(tableId, clientTableConfig);
                tables.putIfAbsent(tableId, clientTable);   //not thread safe
                log.info("reply app thread " + String.valueOf(senderId.intValue));
                commBus.sendInproc(senderId.intValue, msgBuf);

            }
            {
                ByteBuffer msgBuf = null;
                IntBox senderId = new IntBox();
                try {
                    msgBuf = (ByteBuffer) commBusRecvAny.invoke(commBus, senderId);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                int msgType = new NumberedMsg(msgBuf).getMsgType();
                Preconditions.checkArgument(msgType == NumberedMsg.K_CREATED_ALL_TABLES);
            }
        }

        private static void recvAppInitThreadConnection(IntBox numConnectedAppThreads) {
            IntBox senderId = new IntBox();
            while (true) {
                int msgType = new NumberedMsg(commBus.recvInproc(senderId)).getMsgType();
                log.info("received a message with type " + String.valueOf(msgType) + " from " + String.valueOf(senderId.intValue));
                //Preconditions.checkArgument(msgType == NumberedMsg.K_APP_CONNECT);
                if(msgType == NumberedMsg.K_APP_CONNECT)break;
            }
            numConnectedAppThreads.intValue++;
            Preconditions.checkArgument(
                    numConnectedAppThreads.intValue <= GlobalContext.getNumAppThreads());
        }

        /**
         * Connect to namenode and each server
         */
        private static void bgServerHandshake() {
            //connect to the namenode
            int nameNodeId = GlobalContext.getNameNodeId();
            connectToNameNodeOrServer(nameNodeId);
            //wait for connectServerMsg
            ByteBuffer msgBuf = null;
            IntBox senderId = new IntBox();
            if(commBus.isLocalEntity(nameNodeId)){
                msgBuf = commBus.recvInproc(senderId);
            }else{
                msgBuf = commBus.recvInterproc(senderId);
            }
            int msgType = new NumberedMsg(msgBuf).getMsgType();
            Preconditions.checkArgument(senderId.intValue == nameNodeId);
            Preconditions.checkArgument(msgType == NumberedMsg.K_CONNECT_SERVER);

            //connect to servers
            int numServers = GlobalContext.getNumServers();
            int[] serverIds = GlobalContext.getServerIds();
            for (int serverId : serverIds){
                connectToNameNodeOrServer(serverId);
            }

            //get message from servers for permission to start
            for (int numStartedServers = 0; numStartedServers < GlobalContext.getNumServers();
                 numStartedServers++){
                ByteBuffer msgBuf_ = null;
                IntBox senderId_ = new IntBox();
                try {
                    msgBuf_ = (ByteBuffer) commBusRecvAny.invoke(commBus, senderId_);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                int msgType_ = new NumberedMsg(msgBuf_).getMsgType();
                Preconditions.checkArgument(msgType_ == NumberedMsg.K_CLIENT_START);
            }
        }

        private static void connectToNameNodeOrServer(int serverId) {
            ClientConnectMsg clientConnectMsg = new ClientConnectMsg(null);
            int clientId = GlobalContext.getClientId();
            clientConnectMsg.setClientId(clientId);
            ByteBuffer msg = clientConnectMsg.getByteBuffer();

            if (commBus.isLocalEntity(serverId)){
                commBus.connectTo(serverId, msg);
            }else {
                HostInfo serverInfo = GlobalContext.getHostInfo(serverId);
                String serverAddr = new String(serverInfo.ip+":"+serverInfo.port);
                commBus.connectTo(serverId, serverAddr, msg);
            }
        }

        private static void initCommBus(int myId) {
            CommBus.Config commConfig = new CommBus.Config();
            commConfig.entityId = myId;
            commConfig.lType = CommBus.K_IN_PROC;
            commBus.threadRegister(commConfig);
        }

        /**
         * initialize local storage
         */
        private static void initBgContext() {
            bgContext.set(new BgContext());
            bgContext.get().version = 0;
            switch (GlobalContext.getConsistencyModel()){
                case SSP:
                    bgContext.get().rowRequestOpLogMgr = new SSPRowRequestOpLogMgr();
                    break;
                case SSPPush:
                    bgContext.get().rowRequestOpLogMgr = new SSPPushRowRequestOpLogMgr();
                    break;
                default:
            }
            int[] serverIds = GlobalContext.getServerIds();
            for (int serverId : serverIds){
                bgContext.get().serverTableOpLogSizeMap.put(serverId, new HashMap<Integer, Integer>());
                bgContext.get().serverOpLogMsgMap.put(serverId, null);
                bgContext.get().serverOpLogMsgSizeMap.put(serverId, null);
                bgContext.get().tableServerOpLogSizeMap.put(serverId, 0);
                bgContext.get().serverVectorClock.addClock(serverId, 0);
            }
        }
    }
}
