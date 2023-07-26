package com.charls.mydb.backend.vm;

import com.charls.mydb.backend.common.SubArray;
import com.charls.mydb.backend.dm.dataItem.DataItem;
import com.charls.mydb.backend.utils.Parser;
import com.google.common.primitives.Bytes;

import java.util.Arrays;

/**
 * VM向上层抽象出Entry类
 * entry结构：
 * [XMIN] [XMAX] [data]
 * 8byte  8byte
 * XMIN：创建该条记录（版本）的事务编号
 * XMAX：是删除该条记录（版本）的事务编号
 *
 * Entry 记录
 */
public class Entry {
    private static final int OF_XMIN = 0;
    private static final int OF_XMAX = OF_XMIN+8;
    private static final int OF_DATA = OF_XMAX+8;

    private long uid;           // 版本id
    private DataItem dataItem;  // 数据项
    private VersionManager vm;  // 事务的版本管理器

    private static Entry newEntry(VersionManager vm, DataItem dataItem, long uid) {
        Entry entry=new Entry();
        entry.uid=uid;
        entry.dataItem=dataItem;
        entry.vm=vm;
        return entry;
    }

    /**
     * 读取一个DataItem 打包成Entry（加载Entry）
     * @param vm
     * @param uid
     * @return
     * @throws Exception
     */
    public static Entry loadEntry(VersionManager vm,long uid) throws Exception {
        DataItem di=((VersionManagerImpl)vm).dm.read(uid);
        return newEntry(vm,di,uid);
    }

    /**
     * 将事务id和数据记录打包成一个 Entry格式
     * @param xid 事务id
     * @param data 记录数据
     * @return
     */
    public static byte[] wrapEntryRaw(long xid, byte[] data) {
        byte[] xmin = Parser.long2Byte(xid);
        byte[] xmax = new byte[8];
        return Bytes.concat(xmin, xmax, data);
    }

    /**
     * 释放Entry缓存
     */
    public void release(){
        ((VersionManagerImpl)vm).releaseEntry(this);
    }

    /**
     * 释放 DataItem 缓存
     */
    public void remove(){
        dataItem.release();
    }

    /**
     * 以拷贝的形式返回内容
     * @return
     */
    public byte[] data() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            byte[] data = new byte[sa.end - sa.start - OF_DATA];
            System.arraycopy(sa.raw, sa.start+OF_DATA, data, 0, data.length);
            return data;
        } finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 获取 创建该条记录（版本）的事务编号
     * @return
     */
    public long getXmin() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start+OF_XMIN, sa.start+OF_XMAX));
        } finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 获取 删除该条记录（版本）的事务编号
     * @return
     */
    public long getXmax() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start+OF_XMAX, sa.start+OF_DATA));
        } finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 设置 删除该条记录（版本）的事务编号
     * @param xid
     */
    public void setXmax(long xid) {
        dataItem.before();
        try {
            SubArray sa = dataItem.data();
            System.arraycopy(Parser.long2Byte(xid), 0, sa.raw, sa.start+OF_XMAX, 8);
        } finally {
            dataItem.after(xid);
        }
    }

    /**
     * 获取版本id
     * @return
     */
    public long getUid() {
        return uid;
    }
}
