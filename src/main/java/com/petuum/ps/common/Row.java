package com.petuum.ps.common;

import java.util.Map;

/**
 * Created by Su Yuxin on 2014/7/23.
 */
public interface Row {
    void init(int capacity);
    int getUpdateSize();
    int getSerializedSize();
    int serialize(Byte[] bytes);
    boolean deserialize(Byte[] bytes);
    void applyInc(int columnID, Object[] update);
    void applyBatchInc(Map<Integer, Object> batch);
    void applyIncUnsafe(int columnID, Object update);
    void applyBatchIncUnsafe(Map<Integer, Object> batch);
    void addUpdates(int columnID, Object update1, Object update2);
    void subtractUpdate(int columnID, Object update1, Object update2);
    void initUpdate(int columnID, Object zero);
}
