package com.charls.mydb.backend.dm;

import com.charls.mydb.backend.common.SubArray;
import com.charls.mydb.backend.dm.pageCache.PageCache;
import com.charls.mydb.backend.dm.dataItem.DataItem;
import com.charls.mydb.backend.dm.logger.Logger;
import com.charls.mydb.backend.dm.page.Page;
import com.charls.mydb.backend.dm.page.PageX;
import com.charls.mydb.backend.tm.TransactionManager;
import com.charls.mydb.backend.utils.Panic;
import com.charls.mydb.backend.utils.Parser;
import com.google.common.primitives.Bytes;

import java.util.*;

/**
 * recover 例程主要也是两步：重做所有已完成事务 redo，撤销所有未完成事务undo：
 * 两个日志格式：
 *      updateLog:
 *      [LogType] [XID] [UID] [OldRaw] [NewRaw]
 *      insertLog:
 *      [LogType] [XID] [Pgno] [Offset] [Raw]
 */
public class Recover {

    // 日志类型 insert=0 | update=1
    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;

    // REDO 正序扫描，重做日志记录的操作    UNDO 倒序扫描，撤销日志记录的操作
    private static final int REDO = 0;
    private static final int UNDO = 1;

    static class InsertLogInfo{
        long xid;
        int pgno;
        short offset;
        byte[]raw;
    }

    static class UpdateLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    /**
     * recover 例程主要也是两步：重做所有已完成事务 redo，撤销所有未完成事务undo
     * @param tm
     * @param lg
     * @param pc
     */
    public static void recover(TransactionManager tm, Logger lg, PageCache pc) {
        System.out.println("Recovering...");

        // 重置position位置到第一条日志
        lg.rewind();
        int maxPgno = 0;
        while(true) {
            // 获取一条日志数据，同时将 position 移动到下一条日志位置
            byte[] log = lg.next();
            // true ==> 文件全部读取，循环结束
            if(log == null) {
                break;
            }
            int pgno;
            // 根据日志的操作类型
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                pgno = li.pgno;
            } else {
                UpdateLogInfo li = parseUpdateLog(log);
                pgno = li.pgno;
            }
            if(pgno > maxPgno) {
                maxPgno = pgno;
            }
        }
        if(maxPgno == 0) {
            maxPgno = 1;
        }
        // 截断文件到指定页
        pc.truncateByBgno(maxPgno);
        System.out.println("Truncate to " + maxPgno + " pages.");

        // 重做所有已完成事务 redo
        redoTranscations(tm, lg, pc);
        System.out.println("Redo Transactions Over.");

        // 撤销所有未完成事务undo
        undoTranscations(tm, lg, pc);
        System.out.println("Undo Transactions Over.");

        System.out.println("Recovery Over.");
    }

    /**
     * 正序  重做事务处理
     * @param tm 事务管理器
     * @param lg 日志
     * @param pc 页面缓存
     */
    private static void redoTranscations(TransactionManager tm, Logger lg, PageCache pc){
        // 日志文件指针指向第一条记录
        lg.rewind();

        // 无限循环遍历日志，进行重做操作
        while (true){
            byte[] log = lg.next();
            if (log==null){
                break;
            }
            if (isInsertLog(log)){
                // 如果是一条插入日志，则进行doInsertLog的重做操作
                InsertLogInfo li=parseInsertLog(log);
                long xid = li.xid;
                if (!tm.isActive(xid)){
                    // 此事务在数据库崩溃的时候必须是非活跃状态，也就是已提交状态才能进行重做
                    doInsertLog(pc,log,REDO);
                }
            }else {
                // 如果是一条更新日志，则进行doUpdateLog的重做操作
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if(!tm.isActive(xid)) {
                    // 此事务在数据库崩溃的时候必须是非活跃状态，也就是已提交状态才能进行重做
                    doUpdateLog(pc, log, REDO);
                }
            }
        }
    }

    /**
     * 倒序   撤销事务处理
     * @param tm 事务
     * @param lg 日志
     * @param pc 页面缓存
     */
    private static void undoTranscations(TransactionManager tm, Logger lg, PageCache pc) {
        // 日志缓存，key:事务ID   value:日志DATA内容的List      方便后面一次性撤销操作
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        // 日志文件指针指向第一条记录
        lg.rewind();

        // 无限循环遍历日志，进行撤销操作
        while(true) {
            byte[] log = lg.next();
            if(log == null) {
                break;
            }
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);        // 解析为InsertLog格式
                long xid = li.xid;
                if(tm.isActive(xid)) {
                    // 事务处于为提交状态，才能进行撤销
                    if(!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);                 // 使用一个日志缓存Map记录此事务需要撤销的日志数据
                }
            } else {
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if(tm.isActive(xid)) {
                    if(!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }
        }

        // 对所有active log进行倒序undo，遍历logCache即可
        for(Map.Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            // 获得一个事务的所有日志List
            List<byte[]> logs = entry.getValue();
            // 倒序进行撤销操作
            for (int i = logs.size()-1; i >= 0; i --) {
                byte[] log = logs.get(i);
                if(isInsertLog(log)) {
                    doInsertLog(pc, log, UNDO);
                } else {
                    doUpdateLog(pc, log, UNDO);
                }
            }
            // 将此事务标记为 abort状态
            tm.abort(entry.getKey());
        }
    }


    /**
     * 判断日志记录的操作 true ==> 插入    false ==> 更新
     * insertLog:
     *      [LogType] [XID] [Pgno] [Offset] [Raw]
     * 直接读取日志的第一位状态即可
     * @param log 日志
     */
    private static boolean isInsertLog(byte[] log) {
        return log[0]==LOG_TYPE_INSERT;
    }

    /**
     * updateLog:
     * [LogType] [XID] [UID] [OldRaw] [NewRaw]
     *   1字节    8字节 8字节  8字节
     */
    private static final int OF_TYPE = 0;                       // 日志类型偏移位置
    private static final int OF_XID = OF_TYPE+1;                // 日志的事务ID偏移位置
    private static final int OF_UPDATE_UID = OF_XID+8;          // 日志旧数据的偏移位置
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID+8;   // 日志更新内容的偏移位置

    /**
     * 更新日志（ 将原始的 update 操作数据格式化，返回一条 update 日志数据）
     * @param xid 事务ID
     * @param di 抽象数据
     * @return
     */
    public static byte[] updateLog(long xid, DataItem di) {
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] uidRaw = Parser.long2Byte(di.getUid());
        byte[] oldRaw = di.getOldRaw();
        SubArray raw = di.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw);
    }

    /**
     * 将 update 日志数据解析
     * @param log
     * @return
     */
    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo li = new UpdateLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        li.offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        li.pgno = (int)(uid & ((1L << 32) - 1));
        int length = (log.length - OF_UPDATE_RAW) / 2;
        li.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW+length);
        li.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW+length, OF_UPDATE_RAW+length*2);
        return li;
    }

    private static void doUpdateLog(PageCache pc, byte[] log, int flag) {
        int pgno;
        short offset;
        byte[] raw;
        if(flag == REDO) {
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.newRaw;
        } else {
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.oldRaw;
        }
        Page pg = null;
        try {
            pg = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            PageX.recoverUpdate(pg, raw, offset);
        } finally {
            pg.release();
        }
    }

    /**
     * insertLog:
     *      [LogType] [XID] [Pgno] [Offset] [Raw]
     */
    private static final int OF_INSERT_PGNO = OF_XID+8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO+4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET+2;

    /**
     * 插入日志
     * @param xid
     * @param pg
     * @param raw
     * @return
     */
    public static byte[] insertLog(long xid, Page pg, byte[] raw) {
        byte[] logTypeRaw = {LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] pgnoRaw = Parser.int2Byte(pg.getPageNumber());
        byte[] offsetRaw = Parser.short2Byte(PageX.getFSO(pg));
        return Bytes.concat(logTypeRaw, xidRaw, pgnoRaw, offsetRaw, raw);
    }

    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo li = new InsertLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PGNO));
        li.pgno = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PGNO, OF_INSERT_OFFSET));
        li.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        li.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return li;
    }

    private static void doInsertLog(PageCache pc, byte[] log, int flag) {
        InsertLogInfo li = parseInsertLog(log);
        Page pg = null;
        try {
            pg = pc.getPage(li.pgno);
        } catch(Exception e) {
            Panic.panic(e);
        }
        try {
            if(flag == UNDO) {
                DataItem.setDataItemRawInvalid(li.raw);
            }
            PageX.recoverInsert(pg, li.raw, li.offset);
        } finally {
            pg.release();
        }
    }
}
