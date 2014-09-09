package com.petuum.ps.thread;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by zjc on 2014/8/14.
 */
public class BgOpLog {
    private Map<Integer, BgOpLogPartition> tableOpLogMap;

    public BgOpLog() {
        tableOpLogMap = new HashMap<Integer, BgOpLogPartition>();
    }

    public void add(int tableId, BgOpLogPartition bgOpLogPartition){
        tableOpLogMap.put(tableId, bgOpLogPartition);
    }
    public BgOpLogPartition get(int tableId){
        return tableOpLogMap.get(tableId);
    }
}
