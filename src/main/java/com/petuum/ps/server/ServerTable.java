package com.petuum.ps.server;

import com.google.common.io.ByteStreams;
import com.petuum.ps.common.Row;
import com.petuum.ps.common.TableInfo;
import com.petuum.ps.common.util.ClassRegistry;
import com.petuum.ps.common.util.IntBox;
import com.petuum.ps.common.util.RecordBuff;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.Iq80DBFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.*;

/**
 * Created by admin on 2014/8/7.
 */
public class ServerTable {
    private TableInfo tableInfo;
    private HashMap<Integer, ServerRow> storage;

    // used for appending rows to buffs
    private Iterator<Map.Entry<Integer, ServerRow>> rowIter;
    private ByteBuffer tmpRowBuff;
    private int tmpRowBuffSize;
    private static final int K_TMP_ROW_BUFF_SIZE_INIT = 512;
    private int currRowSize;
    private static Logger log = LogManager.getLogger(ServerTable.class);

    public ServerTable(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
        tmpRowBuffSize = K_TMP_ROW_BUFF_SIZE_INIT;
        storage = new HashMap<Integer, ServerRow>();
    }

    public ServerRow findRow(int rowId){
        ServerRow row = storage.get(rowId);
        return row;         //could be null
    }

    public ServerRow createRow(int rowId){
        int rowType = tableInfo.rowType;
        ClassRegistry<Row> classRegistry = ClassRegistry.getRegistry();
        Row rowData = classRegistry.createObject(rowType);
        rowData.init(tableInfo.rowCapacity);
        storage.put(rowId, new ServerRow(rowData));
        return storage.get(rowId);
    }

    public boolean applyRowOpLog(int rowId, Map<Integer, Double> updates){
        ServerRow serverRow = storage.get(rowId);
        if (serverRow == null){
            return false;
        }
        serverRow.applyBatchInc(updates);
        return true;
    }

    public void initAppendTableToBuffs(){
        rowIter = storage.entrySet().iterator();
        tmpRowBuff = ByteBuffer.allocate(tmpRowBuffSize);
    }

    public boolean appendTableToBuffs(int clientIdStart, Map<Integer, RecordBuff> buffs, IntBox failedBgId,
                                      IntBox failedClientId, boolean resume){
        if(resume){
            Map.Entry<Integer, ServerRow> entry = rowIter.next();
            boolean appendRowSuc = entry.getValue().
                    appendRowToBuffs(clientIdStart, buffs, tmpRowBuff, currRowSize, entry.getKey(),
                            failedBgId, failedClientId);
            if(appendRowSuc == false)
                return false;
            clientIdStart = 0;
        }
        while (rowIter.hasNext()){
            Map.Entry<Integer, ServerRow> entry = rowIter.next();
            if (entry.getValue().noClientSubscribed())
                continue;
            if (entry.getValue().isDirty() == false)
                continue;
            entry.getValue().resetDirty();

            tmpRowBuff = entry.getValue().serialize();
            currRowSize = tmpRowBuff.capacity();
            if (currRowSize > tmpRowBuffSize){
                tmpRowBuffSize = currRowSize;
            }
            boolean appendRowSuc = entry.getValue().appendRowToBuffs(clientIdStart, buffs,
                    tmpRowBuff, currRowSize, entry.getKey(), failedBgId, failedClientId);
            if(appendRowSuc == false){
                log.info("Failed at row " + entry.getKey());
                return false;
            }
        }
        tmpRowBuff.clear();
        return true;
    }
    public void makeSnapshotFileName(String snapshotDir, int serverId, int tableId, int clock,
                                     StringBuffer filename){
        checkArgument(filename.length() == 0);
        filename.append(snapshotDir).append("/server_table").append(".server-").append(serverId)
                .append(".table-").append(tableId).append(".clock-").append(clock).append(".db");
    }

    public void takeSnapshot(String snapshotDir, int serverId, int tableId, int clock){
        StringBuffer dbName = new StringBuffer();
        makeSnapshotFileName(snapshotDir, serverId, tableId, clock, dbName);
        Options options = new Options();
        options.createIfMissing(true);
        try {
            DB db = Iq80DBFactory.factory.open(new File(dbName.toString()), options);
            //for
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void readSnapShot(String resumeDir, int serverId, int tableId, int clock){
        StringBuffer dbName = new StringBuffer();
        makeSnapshotFileName(resumeDir, serverId, tableId, clock, dbName);
        Options options = new Options();
        options.createIfMissing(true);
        try{
            DB db =  Iq80DBFactory.factory.open(new File(dbName.toString()), options);
            //for
        }catch (IOException e){
            e.printStackTrace();
        }
    }
}
