package com.petuum.ps.netty;

/**
 * Created by Yuxin Su on 2014/12/4.
 */
public class MyMessage {
    public int count;
    public String message;

    public MyMessage(int count, String msg) {
        this.count = count;
        this.message = msg;
    }
}
