package com.petuum.ps.common.util;

import com.google.common.util.concurrent.AtomicLongMap;

import java.util.Map;
import java.util.Vector;

/**
 * VectorClock manages a vector of clocks and maintains the minimum of these
 * clocks. This class is single thread (ST) only.
 * Created by zengjichuan on 2014/8/13.
 */
public class VectorClock {
    /**
     * A map containing long values that can be atomically updated. While writes to a
     * traditional Map rely on put(K, V), the typical mechanism for writing to this map
     * is addAndGet(K, long), which adds a long to the value currently associated with K.
     * If a key has not yet been associated with a value, its implicit value is zero.
     */
    private AtomicLongMap<Integer> vecClock;
    /**
     * Slowest client clock
     */
    private int minClock;

    public VectorClock() {
        minClock = -1;
        vecClock = AtomicLongMap.create();
    }
    /**
     * Initialize client_ids.size() client clocks with all of them at time 0.
     * @param ids
     */
    public VectorClock(Vector<Integer> ids){
        minClock = 0;
        vecClock = AtomicLongMap.create();
        int size = ids.size();
        for (int i = 0; i < size; i++) {
            vecClock.put(ids.get(i), 0);
        }
    }
    /**
     * If the tick of this client will change the slowest_client_clock_, then
     * it returns true.
     */
    private boolean isUniqueMin(int id){
        if (vecClock.get(id) != minClock){
            // definitely not the slowest
            return false;
        }
        // check if it is also unique
        int numMin = 0;
        for (Map.Entry<Integer, Long> entry : vecClock.asMap().entrySet()){
            if(entry.getValue() == minClock) ++numMin;
            if(numMin > 1) return false;
        }
        return true;
    }



    /**
     * Add a clock in vector clock with initial timestampe. id must be unique.
     * Return 0 on success, negatives on error (e.g., duplicated id).
     * @param id
     * @param clock
     */
    public void addClock(int id, int clock){
        vecClock.put(id, clock);
        if(minClock == -1 || clock < minClock)
            minClock = clock;
    }

    /**
     * Increment client's clock. Accordingly update slowest_client_clock_.
     * Return the minimum clock if client_id is the slowest thread; 0 if not;
     * negatives for error;
     * @param id
     * @return
     */
    public int tick(int id){
        if (isUniqueMin(id)){
            vecClock.incrementAndGet(id);
            return ++minClock;
        }
        vecClock.incrementAndGet(id);
        return 0;
    }
    //Getters
    public int getClock(int id){
        // it not this key, it will return 0; This may be a problem when in the
        // absence of the queried key
        return (int)vecClock.get(id);
    }
    public int getMinClock(){
        return minClock;
    }
}
