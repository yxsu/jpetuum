package com.petuum.ps.server;

import com.petuum.ps.common.util.IntBox;
import com.petuum.ps.common.util.RecordBuff;
import com.petuum.ps.thread.GlobalContext;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Created by admin on 2014/8/6.
 */
public class CallBackSubs {
    public static final int PETUUM_MAX_NUM_CLIENT = 8;
    private BitSet subscriptions;
    public CallBackSubs() {
        subscriptions = new BitSet(PETUUM_MAX_NUM_CLIENT);
    }
    public boolean subscribe(int clientId){
        boolean bitChanged = false;
        if (!subscriptions.get(clientId)){
            bitChanged = true;
            subscriptions.set(clientId);
        }
        return bitChanged;
    }

    public boolean unsubscribe(int clientId) {
        boolean bitChange = false;
        if(subscriptions.get(clientId)){
            bitChange = true;
            subscriptions.set(clientId);
        }
        return bitChange;
    }

    public boolean appendRowToBuffs(int clientIdSt, Map<Integer, RecordBuff> buffs,
                                    ByteBuffer rowData, int rowSize, int rowId,
                                   IntBox failedBgId, IntBox failedClientId) {
        int headBgId;
        int bgId;
        int clientId;
        for (clientId = clientIdSt; clientId < GlobalContext.getNumClients(); ++clientId){
            if (subscriptions.get(clientId)){
                headBgId = GlobalContext.getHeadBgId(clientId);
                bgId = headBgId + GlobalContext.getBgPartitionNum(rowId);
                boolean suc = buffs.get(bgId).append(rowId, rowData, rowSize);
                if (!suc){
                    failedBgId.intValue = bgId;
                    failedClientId.intValue = clientId;
                    return false;
                }
            }
        }
        return true;
    }
}
