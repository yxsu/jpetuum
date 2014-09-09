package com.petuum.ps.server;


import com.petuum.ps.common.Row;
import com.petuum.ps.common.util.IntBox;
import com.petuum.ps.common.util.RecordBuff;
import org.apache.commons.lang3.SerializationUtils;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
* Disallow copy to avoid shared ownership of row_data.
* Allow move sematic for it to be stored in STL containers.
* Created by Zengjichuan on 2014/8/5.
*/
public class ServerRow {
    private CallBackSubs callBackSubs;
    private Row rowData;
    private int numClientsSubscribed;
    private boolean dirty;

    public ServerRow(boolean dirty) {
        this.callBackSubs = new CallBackSubs();
        this.dirty = dirty;
    }

    public ServerRow(Row rowData) {
        this.callBackSubs = new CallBackSubs();
        this.rowData = rowData;
        this.numClientsSubscribed=0;
        this.dirty=false;
    }
    public void applyBatchInc(Map<Integer, Double> updates){
        rowData.applyBatchIncUnsafe(updates);
        dirty = true;
    }

    public void subscribe(int clientId){
        if(callBackSubs.subscribe(clientId))
            ++numClientsSubscribed;
    }
    public boolean noClientSubscribed(){
        return (numClientsSubscribed==0);
    }
    public void unsubscribe(int clientId){
        if (callBackSubs.unsubscribe(clientId))
            --numClientsSubscribed;
    }
    public boolean appendRowToBuffs(int clientIdSt,
                             Map<Integer, RecordBuff> buffs,
                             ByteBuffer rowData, int rowSize, int rowId,
                             IntBox failedBgId, IntBox failedClientId){
        return callBackSubs.appendRowToBuffs(clientIdSt, buffs, rowData, rowSize, rowId,
                failedBgId, failedClientId);
    }
    public boolean isDirty(){
        return dirty;
    }
    public void resetDirty(){
        dirty = false;
    }

    public ByteBuffer serialize() {
        return ByteBuffer.wrap(SerializationUtils.serialize(rowData));
    }
}
