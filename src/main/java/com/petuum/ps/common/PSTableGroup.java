package com.petuum.ps.common;

import com.petuum.ps.common.client.ClientTable;
import com.petuum.ps.common.client.TableGroup;
import com.petuum.ps.common.util.IntBox;
import com.petuum.ps.common.util.ClassRegistry;

import java.util.concurrent.BrokenBarrierException;

/**
 * Created by suyuxin on 14-8-23.
 */
public class PSTableGroup {
    private static TableGroup tableGroup;

    public static int init(TableGroupConfig config, boolean tableAccess) throws NoSuchMethodException, InterruptedException, BrokenBarrierException {
        IntBox initThreadID = new IntBox(0);
        tableGroup = new TableGroup(config, tableAccess, initThreadID);
        return initThreadID.intValue;
    }

    public static void shutDown() throws InterruptedException {
        tableGroup.shutDown();
        tableGroup = null;
    }

    public static boolean createTable(int tabldID, ClientTableConfig config) {
        return tableGroup.createTable(tabldID, config);
    }

    public static void createTableDone() throws BrokenBarrierException, InterruptedException {
        tableGroup.createTableDone();
    }

    public static void waitThreadRegister() throws BrokenBarrierException, InterruptedException {
        tableGroup.waitThreadRegister();
    }

    public static ClientTable getTableOrDie(int tableID) {
        return tableGroup.getTableOrDie(tableID);
    }

    public static int registerThread() throws BrokenBarrierException, InterruptedException {
        return tableGroup.registerThread();
    }

    public static void deregisterThread() throws InterruptedException {
        tableGroup.deregisterThread();
    }

    public static void clock() {
        tableGroup.clock();
    }

    public static void globalBarrier() {
        tableGroup.globalBarrier();
    }

    public static void registerRow(int rowType, Class clazz) {
        ClassRegistry<Row> classRegistry = ClassRegistry.getRegistry();
        classRegistry.addCreator(rowType, clazz);

    }
}
