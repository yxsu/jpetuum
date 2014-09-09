package com.petuum.ps.thread;

import com.petuum.ps.common.util.IntBox;

import java.util.List;
import java.util.Map;
import java.util.Vector;

/**
 * Managing row requests and OpLogs for SSPPush mode.
 * Use the same logic as SSPRowRequestOpLogMgr to avoid repeated row requests
 * to server.
 * Row requests management is completely separated from oplog management.
 * A BgOpLog is always saved.
 * When the bg worker receives a set of server-pushed rows, it removes
 * OpLogs based on the version number in that msg.
 * Created by ZengJichuan on 2014/8/21.
 */
public class SSPPushRowRequestOpLogMgr implements RowRequestOpLogMgr {
    /**
     * map <table_id, row_id> to a list of requests
     * The list is in increasing order of clock
     */
    private Map<TableRowIndex, List<RowRequestInfo>> pendingRowRequest;

    /**
     * The version number of a request means that all oplogs up to and including
     * this version have been applied to this row.
     */
    private Map<Integer, BgOpLog> versionOpLogMap; //std::list<std::pair<uint32_t, BgOpLog*> > version_oplog_list_;

    private int opLogIterVersionStart;
    private int opLogIterVersionEnd;
    private int opLogIterVersionNext;

    private ServerVersionMgr serverVersionMgr;

    public boolean addRowRequest(RowRequestInfo request, int tableId, int rowId) {
        return false;
    }

    public int informReply(int tableId, int rowId, int clock, int currentVersion, Vector<Integer> appThreadIds) {
        return 0;
    }

    public BgOpLog getOpLog(int version) {
        return null;
    }

    public void informVersionInc() {
        serverVersionMgr.incVersionUpperBound();
    }

    public void serverAcknowledgeVersion(int serverId, int version) {

    }

    public boolean addOpLog(int version, BgOpLog opLog) {
        return false;
    }

    public BgOpLog opLogIterInit(int startVersion, int endVersion) {
        return null;
    }

    public BgOpLog opLogIterNext(IntBox version) {
        return null;
    }
}
