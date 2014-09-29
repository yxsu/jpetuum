package com.petuum.ps.oplog;

import com.petuum.ps.common.util.BoolBox;
import com.petuum.ps.common.util.IntBox;
import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Created by ZengJichuan on 2014/8/30.
 */
public class SerializedOpLogReader {
    private ByteBuffer serializedOpLogBuf;
    private int updateSize;
    /**
     * bytes to be read next
     */
    private int offset;
    /**
     * number of tables that I have not finished
     */
    private int numTableLeft;
    private int currentTableId;
    private int numRowsLeftInCurrentTable;

    private static Logger log = LogManager.getLogger(SerializedOpLogReader.class);

    public SerializedOpLogReader(ByteBuffer serializedOpLogBuf) {
        this.serializedOpLogBuf = serializedOpLogBuf;
    }

    public boolean restart(){
        numTableLeft = serializedOpLogBuf.getInt();
        log.info("SerializedOpLogReader Restart(), num_tables_left = " + numTableLeft);
        if(numTableLeft == 0)
            return false;
        startNewTable();
        return true;
    }

    public HashMap<Integer, Double> next(IntBox tableId, IntBox rowId, BoolBox startedNewTable){
        // I have read all
        if(numTableLeft == 0)   return null;
        startedNewTable.boolValue = false;
        int updateSize = 0;
        HashMap<Integer, Double> updates = null;
        while(true){
            // can read from current row
            if(numRowsLeftInCurrentTable > 0){
                tableId.intValue = currentTableId;
                rowId.intValue = serializedOpLogBuf.getInt();
                updateSize = serializedOpLogBuf.getInt();
                byte[] rowOpLogBytes = new byte[updateSize];
                serializedOpLogBuf.get(rowOpLogBytes, 0, updateSize);
                try {
                    updates = (HashMap<Integer, Double>) SerializationUtils.deserialize(rowOpLogBytes);
                }catch(Exception e) {
                    return null;
                }
                numRowsLeftInCurrentTable--;
                return updates;
            }else{
                numTableLeft --;
                if(numTableLeft > 0){
                    startNewTable();
                    startedNewTable.boolValue = true;
                    continue;
                }else
                    return null;
            }
        }
    }
    private void startNewTable() {
        currentTableId = serializedOpLogBuf.getInt();
        updateSize = serializedOpLogBuf.getInt();
        numRowsLeftInCurrentTable = serializedOpLogBuf.getInt();
        log.info("current_table_id = " + currentTableId + " update_size = "+ updateSize +
                " rows_left_in_current_table_ = "+numRowsLeftInCurrentTable);
    }
}
