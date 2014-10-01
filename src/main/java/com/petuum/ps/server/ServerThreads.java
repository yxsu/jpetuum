package com.petuum.ps.server;

import com.petuum.ps.common.HostInfo;
import com.petuum.ps.common.NumberedMsg;
import com.petuum.ps.common.TableInfo;
import com.petuum.ps.common.comm.CommBus;
import com.petuum.ps.common.consistency.ConsistencyModel;
import com.petuum.ps.common.util.IntBox;
import com.petuum.ps.thread.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Vector;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

/**
 * Created by ZengJichuan on 2014/8/13.
 */

class ServerContext{
    int[] bgThreadIds;
    Server serverObj;
    int numShutdownBgs;
}

public class ServerThreads {

    private static Logger log = LogManager.getLogger(ServerThread.class);
    private static CyclicBarrier initBarrier;
    private static int[] threadIDs;
    private static ArrayList<Thread> threads = new ArrayList<Thread>();
    private static ThreadLocal<ServerContext> serverContext = new ThreadLocal<ServerContext>();
    private static Method commBusRecvAny;
    private static Method commBusRecvTimeOutAny;
    private static Method commBusSendAny;
    private static Method commBusRecvAsyncAny;
    private static Method commBusRecvAnyWrapper;

    private static Method serverPushRow;
    private static Method rowSubscribe;

    private static CommBus comm_bus;

    private static class ConnectionResult {
        public int senderID;
        public boolean isClient;
        public int clientID;
    }

    private static class ServerThread implements Runnable {

        public ServerThread(int threadID) {
            this.threadID = threadID;
        }

        private int threadID;

        public void run() {
            ThreadContext.registerThread(threadID);
            //set up thread-specific server context
            setupServerContext();
            setupCommBus();
            try {
                initBarrier.await();
                initServer(threadID);

                IntBox senderID = new IntBox();
                ByteBuffer msgBuf = null;
                boolean destroy_mem = false;

                while(true) {
                    msgBuf = (ByteBuffer) commBusRecvAnyWrapper.invoke(comm_bus, senderID);
                    int msgType = new NumberedMsg(msgBuf).getMsgType();

                    switch (msgType) {
                        case NumberedMsg.K_CLIENT_SHUT_DOWN:
                            log.info("get ClientShutDown from bg " + String.valueOf(senderID.intValue));
                            if(handleShutDownMsg()) {
                                log.info("Server shutdown");
                                comm_bus.threadDeregister();
                                return;
                            }
                        case NumberedMsg.K_CREATE_TABLE:
                            log.info("get CreateTableMsg from bg " + String.valueOf(senderID.intValue));
                            handleCreateTable(senderID.intValue, new CreateTableMsg(msgBuf));
                            break;
                        case NumberedMsg.K_ROW_REQUEST:
                            log.info("get RowRequestMsg from bg " + String.valueOf(senderID.intValue));
                            handleRowRequest(senderID.intValue, new RowRequestMsg(msgBuf));
                            break;
                        case NumberedMsg.K_CLIENT_SEND_OP_LOG:
                            log.info("get ClientSendOpLog from bg " + String.valueOf(senderID.intValue));
                            handleOpLogMsg(senderID.intValue, new ClientSendOpLogMsg(msgBuf));
                            break;
                        default:
                            log.error("Unrecognized message type " + String.valueOf(msgType));
                    }

                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    public static void shutdown() throws InterruptedException {
        for(int i = 0; i < GlobalContext.getNumLocalServerThreads(); i++) {
            threads.get(i).join();
        }
    }

    public static void init(int idST) throws NoSuchMethodException, BrokenBarrierException, InterruptedException {

        initBarrier = new CyclicBarrier(GlobalContext.getNumLocalServerThreads() + 1);
        threadIDs = new int[GlobalContext.getNumLocalServerThreads()];
        comm_bus = GlobalContext.commBus;

        if(GlobalContext.getNumClients() == 1) {
            commBusRecvAny = CommBus.class.getMethod("recvInproc", IntBox.class);
            commBusRecvAsyncAny = CommBus.class.getMethod("recvInprocAsync", IntBox.class);
            commBusRecvTimeOutAny = CommBus.class.getMethod("recvInprocTimeout", IntBox.class, long.class);
            commBusSendAny = CommBus.class.getMethod("sendInproc", int.class, ByteBuffer.class);
        }else {
            commBusRecvAny = CommBus.class.getMethod("recv", IntBox.class);
            commBusRecvAsyncAny = CommBus.class.getMethod("recvAsync", IntBox.class);
            commBusRecvTimeOutAny = CommBus.class.getMethod("recvTimeOut", IntBox.class, long.class);
            commBusSendAny = CommBus.class.getMethod("send", int.class, ByteBuffer.class);
        }

        //
        ConsistencyModel consistency_model = GlobalContext.getConsistencyModel();
        switch(consistency_model) {
            case SSP:
                serverPushRow = ServerThreads.class.getMethod("SSPPushServerPushRow");
                rowSubscribe = ServerThreads.class.getMethod("SSPRowSubscribe", ServerRow.class, int.class);
                break;
            case SSPPush:
                serverPushRow = ServerThreads.class.getMethod("SSPPushServerPushRow");
                rowSubscribe = ServerThreads.class.getMethod("SSPPushRowSubscribe", ServerRow.class, int.class);
                log.info("RowSubscribe = SSPPushRowSubscribe");
                break;
            default:
                log.error("Unrecognized consistency model " + consistency_model.toString());
        }

        if(GlobalContext.isAggressiveCpu()) {
            commBusRecvAnyWrapper = ServerThreads.class.getMethod("commBusRecvAnyBusy", IntBox.class);
        } else {
            commBusRecvAnyWrapper = ServerThreads.class.getMethod("commBusRecvAnySleep", IntBox.class);
        }

        for(int i = 0; i < GlobalContext.getNumLocalServerThreads(); i++) {
            threadIDs[i] = idST + i;
            log.info("Create server thread " + String.valueOf(i));
            threads.add(new Thread(new ServerThread(idST + i)));
            threads.get(i).start();
        }
        initBarrier.await();
    }

    public static void SSPPushServerPushRow() throws NoSuchMethodException {

        serverContext.get().serverObj.createSendServerPushRowMsgs(ServerThreads.class.getMethod("sendServerPushRowMsg", int.class, ServerPushRowMsg.class, boolean.class));

    }

    public static ByteBuffer commBusRecvAnyBusy(IntBox senderId) throws InvocationTargetException, IllegalAccessException {
        ByteBuffer buffer = (ByteBuffer)commBusRecvAsyncAny.invoke(comm_bus, senderId);
        while(buffer == null) {
            buffer = (ByteBuffer)commBusRecvAsyncAny.invoke(comm_bus, senderId);
        }
        return buffer;
    }

    public static ByteBuffer commBusRecvAnySleep(IntBox senderId) throws InvocationTargetException, IllegalAccessException {
        return (ByteBuffer)commBusRecvAny.invoke(comm_bus, senderId);
    }

    public static void SSPPushRowSubscribe(ServerRow serverRow, int clientId){
        serverRow.subscribe(clientId);
    }

    public static void SSPRowSubscribe(ServerRow serverRow, int clientId) {

    }
    // communication function
    // assuming the caller is not name node
    private static void connectToNameNode(){
        int nameNodeID = GlobalContext.getNameNodeId();
        ServerConnectMsg msg = new ServerConnectMsg(null);

        if(comm_bus.isLocalEntity(nameNodeID)) {
            log.info("Connect to local name node");
            comm_bus.connectTo(nameNodeID, msg.getByteBuffer());
        } else {
            log.info("Connect to remote name node");
            HostInfo nameNodeInfo = GlobalContext.getHostInfo(nameNodeID);
            String nameNodeAddr = nameNodeInfo.ip + ":" + nameNodeInfo.port;
            log.info("name_node_addr = " + String.valueOf(nameNodeAddr));
            comm_bus.connectTo(nameNodeID, nameNodeAddr, msg.getByteBuffer());
        }
    }

    private static ConnectionResult getConnection() throws InvocationTargetException, IllegalAccessException {
        IntBox senderID = new IntBox();
        ConnectionResult result = new ConnectionResult();
        ByteBuffer msgBuf = (ByteBuffer) commBusRecvAny.invoke(comm_bus, senderID);
        NumberedMsg msg = new NumberedMsg(msgBuf);
        if(msg.getMsgType() == NumberedMsg.K_CLIENT_CONNECT) {
            ClientConnectMsg cMsg = new ClientConnectMsg(msgBuf);
            result.isClient = true;
            result.clientID = cMsg.getClientID();
        } else {
            assert msg.getMsgType() == NumberedMsg.K_SERVER_CONNECT;
            result.isClient = false;
        }
        result.senderID = senderID.intValue;
        return result;
    }

/**
     * Functions that operate on the particular thread's specific ServerContext.
     */

    private static void setupServerContext(){

        serverContext.set(new ServerContext());
        serverContext.get().bgThreadIds = new int[GlobalContext.getNumTotalBgThreads()];
        serverContext.get().numShutdownBgs = 0;
        serverContext.get().serverObj = new Server();

    }
    private static void setupCommBus(){

        int myID = ThreadContext.getId();
        CommBus.Config config = new CommBus.Config();
        config.entityId = myID;
        log.info("ServerThreads num_clients = " + String.valueOf(GlobalContext.getNumClients()));
        log.info("my id = " + String.valueOf(myID));

        if(GlobalContext.getNumClients() > 1) {
            config.lType = CommBus.K_IN_PROC | CommBus.K_INTER_PROC;
            HostInfo hostInfo = GlobalContext.getHostInfo(myID);
            config.networkAddr = hostInfo.ip + ":" + hostInfo.port;
            log.info("network addr = " + config.networkAddr);
        } else {
            config.lType = CommBus.K_IN_PROC;
        }

        comm_bus.threadRegister(config);
        log.info("Server thread registered CommBus");

    }
    private static void initServer(int serverId) throws InvocationTargetException, IllegalAccessException, InterruptedException {

        connectToNameNode();

        for(int numBgs = 0; numBgs < GlobalContext.getNumTotalBgThreads(); numBgs++) {
            ConnectionResult result = getConnection();
            assert result.isClient;
            serverContext.get().bgThreadIds[numBgs] = result.senderID;
            serverContext.get().serverObj.addClientBgPair(result.clientID, result.senderID);
        }

        serverContext.get().serverObj.init(serverId);
        sendToAllBgThreads(new ClientStartMsg(null));
        log.info("InitNonNameNode done");

    }
    private static void sendToAllBgThreads(NumberedMsg msg) throws InvocationTargetException, IllegalAccessException {
        for(int i = 0; i < GlobalContext.getNumTotalBgThreads(); i++) {
            int bgId = serverContext.get().bgThreadIds[i];
            commBusSendAny.invoke(comm_bus, bgId, msg.getByteBuffer());
            log.info("send message with type " + String.valueOf(msg.getMsgType()) + " to " + String.valueOf(bgId));
            while(!(Boolean)commBusSendAny.invoke(comm_bus, bgId, msg.getByteBuffer()))
            {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    private static boolean handleShutDownMsg() throws InvocationTargetException, IllegalAccessException {
        int numShutdownBgs = serverContext.get().numShutdownBgs;
        numShutdownBgs++;
        if(numShutdownBgs == GlobalContext.getNumTotalBgThreads()) {
            ServerShutDownAckMsg shutDownAckMsg = new ServerShutDownAckMsg(null);
            for(int i = 0; i < GlobalContext.getNumTotalBgThreads(); i++) {
                int bgId = serverContext.get().bgThreadIds[i];
                commBusSendAny.invoke(comm_bus, bgId, shutDownAckMsg.getByteBuffer());
            }
            return true;
        }
        return false;
    }
    private static void handleCreateTable(int senderId, CreateTableMsg createTableMsg) throws InvocationTargetException, IllegalAccessException {

        int tableId = createTableMsg.getTableId();

        CreateTableReplyMsg createTableReplyMsg = new CreateTableReplyMsg(null);
        createTableReplyMsg.setTableId(tableId);
        commBusSendAny.invoke(comm_bus, senderId, createTableReplyMsg.getByteBuffer());

        TableInfo tableInfo = new TableInfo();
        tableInfo.tableStaleness = createTableMsg.getStaleness();
        tableInfo.rowType = createTableMsg.getRowType();
        tableInfo.rowCapacity = createTableMsg.getRowCapacity();
        serverContext.get().serverObj.CreateTable(tableId, tableInfo);

    }
    private static void handleRowRequest(int senderId, RowRequestMsg rowRequestMsg) throws InvocationTargetException, IllegalAccessException {
        int tableId = rowRequestMsg.getTableId();
        int rowId = rowRequestMsg.getRowId();
        int start = rowRequestMsg.getStart();
        int offset = rowRequestMsg.getOffset();
        int clock = rowRequestMsg.getClock();
        int serverClock = serverContext.get().serverObj.getMinClock();
        if(serverClock < clock) {
            serverContext.get().serverObj.addRowRequest(senderId, tableId, rowId, start, offset, clock);
            return;
        }

        int version = serverContext.get().serverObj.getBgVersion(senderId);

        ServerRow serverRow = serverContext.get().serverObj.findCreateRow(tableId, rowId, start, offset);
        rowSubscribe.invoke(ServerThreads.class, serverRow, GlobalContext.threadId2ClientId(senderId));
        replyRowRequest(senderId, serverRow, tableId, rowId, serverClock, version);
    }
    private static void replyRowRequest(int bgId, ServerRow serverRow, int tableId,
                                        int rowId, int serverClock, int version){

        ByteBuffer serverRowBuffer = serverRow.serialize();
        ServerRowRequestReplyMsg serverRowRequestReplyMsg = new ServerRowRequestReplyMsg(null);
        serverRowRequestReplyMsg.setRowData(serverRowBuffer);       //need to set first
        serverRowRequestReplyMsg.setTableId(tableId);
        serverRowRequestReplyMsg.setRowId(rowId);
        serverRowRequestReplyMsg.setClock(serverClock);
        serverRowRequestReplyMsg.setVersion(version);
        serverRowRequestReplyMsg.setRowSize(serverRowBuffer.capacity());

        log.info("Replying client row request, version = "+version+" table id = "+tableId);
        //TransferMem ...
        if(comm_bus.isLocalEntity(bgId)){
            comm_bus.sendInproc(bgId, serverRowRequestReplyMsg.getByteBuffer());
        }else{
            comm_bus.sendInterproc(bgId, serverRowRequestReplyMsg.getByteBuffer());
        }
    }
    private static void handleOpLogMsg(int senderId, ClientSendOpLogMsg clientSendOpLogMsg) throws InvocationTargetException, IllegalAccessException {

        int clientId = clientSendOpLogMsg.getClientId();
        boolean isClock = clientSendOpLogMsg.getIsClock();
        int version = clientSendOpLogMsg.getVersion();

        serverContext.get().serverObj.applyOpLog(clientSendOpLogMsg.getData(), senderId, version);

        if(isClock) {
            boolean clockChanged = serverContext.get().serverObj.clock(clientId, senderId);
            if(clockChanged) {
                Vector<ServerRowRequest> requests = new Vector<ServerRowRequest>();
                serverContext.get().serverObj.getFulfilledRowRequests(requests);
                for(ServerRowRequest request : requests) {
                    int tableId = request.tableId;
                    int rowId = request.rowId;
                    int columnId = request.columnId;
                    int offset = request.offset;
                    int bgId = request.bgId;
                    int version2 = serverContext.get().serverObj.getBgVersion(bgId);
                    ServerRow serverRow = serverContext.get().serverObj.findCreateRow(tableId, rowId, columnId, offset);
                    rowSubscribe.invoke(ServerThreads.class, serverRow, GlobalContext.threadId2ClientId(bgId));
                    int serverClock = serverContext.get().serverObj.getMinClock();
                    replyRowRequest(bgId, serverRow, tableId, rowId, serverClock, version2);
                }
                serverPushRow.invoke(ServerThreads.class);
            }
        }
    }
    public static void sendServerPushRowMsg(int bgId, ServerPushRowMsg msg, boolean lastMsg) throws InvocationTargetException, IllegalAccessException {

        msg.setVersion(serverContext.get().serverObj.getBgVersion(bgId));

        if(lastMsg) {
            msg.setIsClock(true);
            msg.setClock(serverContext.get().serverObj.getMinClock());
        } else {
            msg.setIsClock(false);
            commBusSendAny.invoke(comm_bus, bgId, msg.getByteBuffer());
        }

    }

}
