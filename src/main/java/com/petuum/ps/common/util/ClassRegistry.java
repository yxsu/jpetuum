package com.petuum.ps.common.util;

import java.lang.reflect.Constructor;
import java.util.HashMap;

/**
 * factory method
 * Created by ZengJichuan on 2014/8/29.
 */
public class ClassRegistry <BaseClass>{
    private final static ClassRegistry instance = new ClassRegistry();
    private HashMap<Integer, Class> createMap;
    private ClassRegistry(){
        createMap = new HashMap<Integer, Class>();
    }
    public static <BaseClass> ClassRegistry<BaseClass> getRegistry(){
        return instance;
    }
    public void addCreator(int key, Class productClass){
        createMap.putIfAbsent(key, productClass);
    }
    public BaseClass createObject(int key){
        Class productClass = createMap.get(key);
        BaseClass product = null;
        try {
            Constructor productConstructor = productClass.getConstructor();
            product = (BaseClass)productConstructor.newInstance();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return product;
    }
}
