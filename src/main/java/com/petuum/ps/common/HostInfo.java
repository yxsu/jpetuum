package com.petuum.ps.common;

/**
 * Created by admin on 2014/8/13.
 */
public class HostInfo {
    public int id;
    public String ip;
    public String port;

    public HostInfo() {
    }

    public HostInfo(int id, String ip, String port) {
        this.id = id;
        this.ip = ip;
        this.port = port;
    }
}
