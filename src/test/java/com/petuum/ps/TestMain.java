package com.petuum.ps;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.Assert.assertTrue;

/**
 * Created by Su Yuxin on 2014/7/24.
 */
public class TestMain {
    @Test
    public void speed() {
        int size = 10000000;
        Map<Integer, Double> test_hash = new HashMap<Integer, Double>();
        Map<Integer, Double> test_tree = new TreeMap<Integer, Double>();
        Random rand = new Random();
        //test hash
        long start = System.nanoTime();
        long start_mill = System.currentTimeMillis();
        for(int i = 0; i < size; i++) {
            test_hash.put(i, rand.nextDouble());
        }
        double time_hash = (System.nanoTime() - start) / 1e9f;
        test_hash = null;
        //test tree
        start = System.nanoTime();
        for(int i = 0; i < size; i++) {
            test_tree.put(i, rand.nextDouble());
        }
        double time_tree = (System.nanoTime() - start) / 1e9f;
        test_tree = null;
        System.out.println("Running time of TreeMap is " + String.valueOf(time_tree) + "s");
        System.out.println("Running time of HashMap is " + String.valueOf(time_hash) + "s");
        assertTrue(time_tree > time_hash);
    }
}
