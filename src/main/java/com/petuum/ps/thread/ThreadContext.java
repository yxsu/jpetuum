package com.petuum.ps.thread;

/**
 * Created by zjc on 2014/8/14.
 */
public class ThreadContext {
    private static ThreadLocal<Info> threadInfo = new ThreadLocal<Info>();
    private static class Info{
        int entityId;
        int clock;
        Info(int entityId){
            this.entityId = entityId;
            clock = 0;
        }
    }
    public static void registerThread(int threadId){
        threadInfo.set(new Info(threadId));
    }
    public static int getId(){
        return threadInfo.get().entityId;
    }
    public static int getClock(){
        return threadInfo.get().clock;
    }
    public static void clock(){
        threadInfo.get().clock++;
    }
}
