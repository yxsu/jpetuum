package com.petuum.ps.common.test;

import org.apache.commons.lang3.SerializationUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by suyuxin on 14-8-30.
 */
public class CaseTest {
    public static void main(String[] args) {
        HashMap<Integer, Double> test = new HashMap<Integer, Double>();
        for(int i = 0; i < 100; i++) {
            System.out.println(String.valueOf(i) + " : " + SerializationUtils.serialize(test).length);
            test.putIfAbsent(i, i * 1.1);
        }
    }
}
