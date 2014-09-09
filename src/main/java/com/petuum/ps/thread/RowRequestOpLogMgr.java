package com.petuum.ps.thread;

import com.petuum.ps.common.util.IntBox;

import java.util.Vector;

/**
 * Created by ZengJichuan on 2014/8/20.
 */
public interface RowRequestOpLogMgr {
    /**
     * return true unless there's a previous request with lower or same clock number
     * @param request
     * @param tableId
     * @param rowId
     * @return
     */
    public boolean addRowRequest(RowRequestInfo request, int tableId, int rowId);

    /**
     * Get a list of app thread ids that can be satisfied with this reply.
     * Corresponding row requests are removed upon returning.
     * If all row requests prior to some version are removed, those OpLogs are
     * removed as well.
     * @param tableId
     * @param rowId
     * @param clock
     * @param currentVersion
     * @param appThreadIds
     * @return
     */
    public int informReply(int tableId, int rowId, int clock, int currentVersion, Vector<Integer> appThreadIds);

    /**
     * Get OpLog of a particular version.
     * @param version
     * @return
     */
    public BgOpLog getOpLog(int version);
    public void informVersionInc();
    public void serverAcknowledgeVersion(int serverId, int version);
    public boolean addOpLog(int version, BgOpLog opLog);

    public BgOpLog opLogIterInit(int startVersion, int endVersion);
    public BgOpLog opLogIterNext(IntBox version);
}

class RowRequestInfo{
    public int appThreadId;
    public int clock;
    public int version;
    boolean sent;

    RowRequestInfo() {
        this.appThreadId = 0;
        this.clock = 0;
        this.version = 0;
    }
}