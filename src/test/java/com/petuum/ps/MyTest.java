package com.petuum.ps;

import com.google.common.util.concurrent.AtomicLongMap;
import com.petuum.ps.common.NumberedMsg;
import com.petuum.ps.common.util.IntBox;

import java.util.HashMap;

import java.nio.*;

import java.util.Objects;

/**
 * Created by admin on 2014/8/6.
 */
public class MyTest {

    public static void sum(IntBox one) {
        one.intValue++;
    }
    public static void main(String args[]){
        ByteBuffer one = ByteBuffer.allocate(4);
        one.putInt(10);
        System.out.println(one.getInt(0));

//        sum(tint);
//
//
//        System.out.println(tint.intValue);

//        NumberedMsg mt = NumberedMsg.valueOf(Objects.toString(NumberedMsg.K_APP_CONNECT));
//        if(mt.equals(NumberedMsg.K_APP_CONNECT)){
//            System.out.println("Success");
//        }

        //test Map value increment
        AtomicLongMap<Integer> gMap = AtomicLongMap.create();
        HashMap<Integer, Integer> hMap = new HashMap<Integer, Integer>();
        long gst = System.nanoTime();
        for (int i = 0; i < 100000; i++) {
            gMap.getAndIncrement(i%1000);
        }
        long gen = System.nanoTime();
        for (int i = 0; i <100000; i++) {
            if(!hMap.containsKey(i%1000))
                hMap.put(i%1000, 0);
            hMap.put(i%1000, hMap.get(i%1000)+1);
        }
        long hen = System.nanoTime();
        System.out.println("Gmap: " + (gen - gst)/1e9f);        //Gmap: 0.0627443
        System.out.println("Hmap: " + (hen - gen)/1e9f);        //Hmap: 0.08728583
    }

}
