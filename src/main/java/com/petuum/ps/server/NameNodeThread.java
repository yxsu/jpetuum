package com.petuum.ps.server;

import com.petuum.ps.common.HostInfo;
import com.petuum.ps.common.NumberedMsg;
import com.petuum.ps.common.TableInfo;
import com.petuum.ps.common.comm.CommBus;
import com.petuum.ps.common.util.IntBox;
import com.petuum.ps.thread.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

/**
 * Created by suyuxin on 14-8-23.
 */

class CreateTableInfo {
    public int numClientsReplied;
    public int numServersReplied;
    public Queue<Integer> bgsToReply = new ArrayDeque<Integer>();

    public boolean receviedFromAllServers() {
        return (numServersReplied == GlobalContext.getNumServers());
    }

    public boolean repliedToAllClients() {
        return (numClientsReplied == GlobalContext.getNumClients());
    }
}

class NameNodeContext {
    public int[] bgThreadIDs;
    public Map<Integer, CreateTableInfo> createTableMap;
    public Server serverObj;
    public int numShutdownBgs;
}

public class NameNodeThread {
    private static CyclicBarrier latch;
    private static ThreadLocal<NameNodeContext> nameNodeContext = new ThreadLocal<NameNodeContext>();

    private static Method commBusRecvAny;
    private static Method commBusRecvTimeOutAny;
    private static Method commBusSendAny;
    private static CommBus commbus;
    private static Logger log = LogManager.getLogger(NameNodeThread.class);

    private static Thread thread = new Thread(new Runnable() {//NameNodeThreadMain
        public void run() {
            int myID = GlobalContext.getNameNodeId();

            ThreadContext.registerThread(myID);

            //set up thread-specific server context
            setupNameNodeContext();
            setupCommBus();

            try {
                latch.await();
                initNameNode();

                while(true) {
                    IntBox senderId = new IntBox();
                    ByteBuffer buffer = (ByteBuffer)commBusRecvAny.invoke(commbus, senderId);
                    int msgType = new NumberedMsg(buffer).getMsgType();
                    log.info("Name Node receives a message with type " + String.valueOf(msgType));

                    switch (msgType) {
                        case NumberedMsg.K_CLIENT_SHUT_DOWN:
                        {
                            log.info("get ClientShutDown from bg " + String.valueOf(senderId.intValue));
                            if(handleShutDownMsg()) {
                                log.info("NameNode shutting down");
                                commbus.threadDeregister();
                                return;
                            }
                            break;
                        }
                        case NumberedMsg.K_CREATE_TABLE:
                        {
                            log.info("get CreateTableMsg from bg " + String.valueOf(senderId.intValue));
                            handleCreateTable(senderId, new CreateTableMsg(buffer));
                            break;
                        }
                        case NumberedMsg.K_CREATE_TABLE_REPLY:
                        {
                            log.info("get CreateTableReplyMsg from bg " + String.valueOf(senderId.intValue));
                            handleCreateTableReply(new CreateTableReplyMsg(buffer));
                            break;
                        }
                        default:
                            log.fatal("Unrecognized message type " + String.valueOf(msgType) + " from " + String.valueOf(senderId.intValue));
                    }

                }
            } catch (InvocationTargetException e) {
                log.error(e.getMessage());
            } catch (IllegalAccessException e) {
                log.error(e.getMessage());
            } catch (InterruptedException e) {
                log.error(e.getMessage());
            } catch (BrokenBarrierException e) {
                log.error(e.getMessage());
            }
        }
    });

    private static class ConnectionResult {
        public int senderID;
        public boolean isClient;
        public int clientID;
    }

    private static void handleCreateTable(IntBox senderId, CreateTableMsg msg) throws InvocationTargetException, IllegalAccessException {
        int tableId = msg.getTableId();
        Map<Integer, CreateTableInfo> createTableMap = nameNodeContext.get().createTableMap;
        if(!createTableMap.containsKey(tableId)) {
            TableInfo tableInfo = new TableInfo();
            tableInfo.tableStaleness = msg.getStaleness();
            tableInfo.rowType = msg.getRowType();
            tableInfo.rowCapacity = msg.getRowCapacity();
            nameNodeContext.get().serverObj.CreateTable(tableId, tableInfo);

            createTableMap.putIfAbsent(tableId, new CreateTableInfo());
            sendToAllServers(msg);
        }
        if(createTableMap.get(tableId).receviedFromAllServers()) {
            CreateTableReplyMsg replyMsg = new CreateTableReplyMsg(null);
            replyMsg.setTableId(msg.getTableId());
            commBusSendAny.invoke(commbus, senderId, replyMsg);
            createTableMap.get(tableId).numClientsReplied++;
            if(haveCreatedAllTables()) {
                sendCreateAllTablesMsg();
            }
        } else {
            createTableMap.get(tableId).bgsToReply.add(senderId.intValue);
        }
    }

    private static void handleCreateTableReply(CreateTableReplyMsg msg) throws InvocationTargetException, IllegalAccessException {
        int tableId = msg.getTableId();
        Map<Integer, CreateTableInfo> createTableMap = nameNodeContext.get().createTableMap;
        createTableMap.get(tableId).numServersReplied++;

        if(createTableMap.get(tableId).receviedFromAllServers()) {
            Queue<Integer> bgsToReply = createTableMap.get(tableId).bgsToReply;
            while(!bgsToReply.isEmpty()) {
                int bgId = bgsToReply.poll();
                commBusSendAny.invoke(commbus, bgId, msg.getByteBuffer());
                createTableMap.get(tableId).numClientsReplied++;
            }
            if(haveCreatedAllTables()) {
                sendCreateAllTablesMsg();
            }
        }
    }

    private static boolean haveCreatedAllTables() {
        Map<Integer, CreateTableInfo> createTableMap = nameNodeContext.get().createTableMap;
        if(createTableMap.size() < GlobalContext.getNumTables())
            return false;

        for(Map.Entry<Integer, CreateTableInfo> entry : createTableMap.entrySet()) {
            if(!entry.getValue().repliedToAllClients())
                return false;
        }
        return true;
    }

    private static void sendCreateAllTablesMsg() throws InvocationTargetException, IllegalAccessException {
        CreateAllTablesMsg createAllTablesMsg = new CreateAllTablesMsg(null);
        int numClients = GlobalContext.getNumClients();

        for(int clientIdx = 0; clientIdx < numClients; clientIdx++) {
            int headBgId = GlobalContext.getHeadBgId(clientIdx);
            commBusSendAny.invoke(commbus, headBgId, createAllTablesMsg.getByteBuffer());
        }
    }

    private static boolean handleShutDownMsg() throws InvocationTargetException, IllegalAccessException {
        int numShutDownBgs = ++(nameNodeContext.get().numShutdownBgs);
        if(numShutDownBgs == GlobalContext.getNumTotalBgThreads()) {
            for(int i = 0; i < GlobalContext.getNumTotalBgThreads(); i++) {
                commBusSendAny.invoke(commbus, nameNodeContext.get().bgThreadIDs[i], new ServerShutDownAckMsg(null).getByteBuffer());
            }
            return true;
        }
        return false;
    }

    public static void init() throws NoSuchMethodException, InterruptedException, BrokenBarrierException {
        latch = new CyclicBarrier(2);
        commbus = GlobalContext.commBus;

        if(GlobalContext.getNumClients() == 1) {
            commBusRecvAny = CommBus.class.getMethod("recvInproc", IntBox.class);
        } else {
            commBusRecvAny = CommBus.class.getMethod("recv", IntBox.class);
        }

        if(GlobalContext.getNumClients() == 1) {
            commBusRecvTimeOutAny = CommBus.class.getMethod("recvInprocTimeout", IntBox.class, long.class);
        } else {
            commBusRecvTimeOutAny = CommBus.class.getMethod("recvTimeOut", IntBox.class, long.class);
        }

        if(GlobalContext.getNumClients() == 1) {
            commBusSendAny = CommBus.class.getMethod("sendInproc", int.class, ByteBuffer.class);
        } else {
            commBusSendAny = CommBus.class.getMethod("send", int.class, ByteBuffer.class);
        }
        thread.start();
        latch.await();
    }

    public static void shutDown() throws InterruptedException {
        thread.join();
    }

    private static void setupNameNodeContext() {
        nameNodeContext.set(new NameNodeContext());
        nameNodeContext.get().bgThreadIDs = new int[GlobalContext.getNumTotalBgThreads()];
        nameNodeContext.get().numShutdownBgs = 0;
        nameNodeContext.get().serverObj = new Server();
        nameNodeContext.get().createTableMap = new HashMap<Integer, CreateTableInfo>();
    }

    private static void setupCommBus() {
        int myID = ThreadContext.getId();
        CommBus.Config config = new CommBus.Config(myID, CommBus.K_IN_PROC, "");

        if(GlobalContext.getNumClients() > 1) {
            config.lType = CommBus.K_IN_PROC | CommBus.K_INTER_PROC;
            HostInfo hostInfo = GlobalContext.getHostInfo(myID);
            config.networkAddr = hostInfo.ip + ":" + hostInfo.port;
        }

        commbus.threadRegister(config);
        log.info("NameNode is ready to accept connections!");
    }

    private static void initNameNode() throws InvocationTargetException, IllegalAccessException {
        int numBgs = 0;
        int numServers = 0;
        int numExpectedConns = GlobalContext.getNumTotalBgThreads() + GlobalContext.getNumServers();
        log.info("Number totalBgThreads() = " + String.valueOf(GlobalContext.getNumTotalBgThreads()));
        log.info("Number totalServerThreads() = " + String.valueOf(GlobalContext.getNumServers()));
        for(int numConnections = 0; numConnections < numExpectedConns; numConnections++) {
            ConnectionResult cResult = getConnection();
            if(cResult.isClient) {
                nameNodeContext.get().bgThreadIDs[numBgs] = cResult.senderID;
                numBgs++;
                nameNodeContext.get().serverObj.addClientBgPair(cResult.clientID, cResult.senderID);
                log.info("Name node get client " + String.valueOf(cResult.senderID));
            } else {
                numServers++;
                log.info("Name node gets server " + String.valueOf(cResult.senderID));
            }
        }

        assert numBgs == GlobalContext.getNumTotalBgThreads();
        nameNodeContext.get().serverObj.init(0);
        log.info("Has received connections from all clients and servers, sending out connectServerMsg");

        sendToAllBgThreads(new ConnectServerMsg(null));
        log.info("Send ConnectServerMsg done");
        sendToAllBgThreads(new ClientStartMsg(null));
        log.info("initNameNode done");
    }

    private static ConnectionResult getConnection() throws InvocationTargetException, IllegalAccessException {
        IntBox senderID = new IntBox();
        ByteBuffer msgBuf = (ByteBuffer) commBusRecvAny.invoke(commbus, senderID);

        NumberedMsg msg = new NumberedMsg(msgBuf);
        ConnectionResult result = new ConnectionResult();
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

    private static void sendToAllBgThreads(NumberedMsg msg) throws InvocationTargetException, IllegalAccessException {
        for(int i = 0; i < GlobalContext.getNumTotalBgThreads(); i++) {
            int bdID = nameNodeContext.get().bgThreadIDs[i];
            if(!(Boolean)commBusSendAny.invoke(commbus, bdID, msg.getByteBuffer())) {
                log.error("fails to send to bg thread with Id = " + String.valueOf(bdID));
            }
        }
    }

    private static void sendToAllServers(NumberedMsg msg) throws InvocationTargetException, IllegalAccessException {
        int[] serverIds = GlobalContext.getServerIds();
        for(int i = 0; i < serverIds.length; i++) {
            commBusSendAny.invoke(commbus, serverIds[i], msg.getByteBuffer());
        }
    }
}
