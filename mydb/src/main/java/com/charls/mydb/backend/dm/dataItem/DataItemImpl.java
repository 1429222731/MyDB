package com.charls.mydb.backend.dm.dataItem;

import com.charls.mydb.backend.common.SubArray;
import com.charls.mydb.backend.dm.DataManagerImpl;
import com.charls.mydb.backend.dm.page.Page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * dataItem 是 页面中指定数据的打包结构，也是具体操作数据的结构
 * dataItem 结构如下：
 * [ValidFlag] [DataSize] [Data]
 * ValidFlag 1字节，0为合法，1为非法
 * DataSize  2字节，标识Data的长度
 *
 * ValidFlag 占用 1 字节，标识了该 DataItem 是否有效。删除一个 DataItem，只需要简单地将其有效位设置为 0。DataSize 占用 2 字节，标识了后面 Data 的长度。
 */
public class DataItemImpl implements DataItem{

    static final int OF_VALID = 0;   // 占用 1 字节，标识了该 DataItem 是否有效。删除一个 DataItem，只需要简单地将其有效位设置为 0。
    static final int OF_SIZE = 1;    // DataSize 占用 2 字节，标识了后面 Data 的长度。
    static final int OF_DATA = 3;

    private SubArray raw;           // 子区间数据，共享内存
    private byte[] oldRaw;          // 暂存需要修改的数据内容
    private Lock rLock;             // 读锁
    private Lock wLock;             // 写锁

    // 保存了一个 dm 的引用是为了释放 依赖 dm 的缓存（dm 同时实现了缓存接口，用于缓存 DataItem），以及修改数据时记录日志
    private DataManagerImpl dm;
    private long uid;               // DataItem缓存的key，uid = 页号 + 偏移量
    private Page pg;                // 数据页

    /**
     * 构造函数
     */
    public DataItemImpl(SubArray raw, byte[] oldRaw, Page pg, long uid, DataManagerImpl dm) {
        this.raw = raw;
        this.oldRaw = oldRaw;

        ReadWriteLock lock = new ReentrantReadWriteLock();
        rLock = lock.readLock();
        wLock = lock.writeLock();

        this.dm = dm;
        this.uid = uid;
        this.pg = pg;
    }

    /**
     * 校验这个DataItem是否合法
     * @return
     */
    public boolean isValid(){
        return raw.raw[raw.start+OF_VALID]==(byte) 0;
    }

    /**
     * 通过共享内存的方式获取指定的 DATA 数据
     * @return
     */
    @Override
    public SubArray data() {
        return new SubArray(raw.raw, raw.start+OF_DATA, raw.end);
    }

    /**
     * 修改数据之前的操作
     * 包含了加写锁，设置脏页面，暂存需要修改的数据内容到oldRaw
     */
    @Override
    public void before() {
        wLock.lock();
        pg.setDirty(true);
        // 浅拷贝
        System.arraycopy(raw.raw, raw.start, oldRaw, 0, oldRaw.length);
    }

    /**
     * 撤销修改
     * 将数据还原，关闭写锁
     */
    @Override
    public void unBefore() {
        System.arraycopy(oldRaw, 0, raw.raw, raw.start, oldRaw.length);
        wLock.unlock();
    }

    /**
     * 修改数据完成后的操作
     * 记录此事务的修改操作到日志，关闭写锁
     * @param xid
     */
    @Override
    public void after(long xid) {
        dm.logDataItem(xid, this);
        wLock.unlock();
    }

    /**
     * 释放这个DataItem的缓存
     */
    @Override
    public void release() {
        dm.releaseDataItem(this);
    }

    /**
     * 打开写锁
     */
    @Override
    public void lock() {
        wLock.lock();
    }

    /**
     * 关闭写锁
     */
    @Override
    public void unlock() {
        wLock.unlock();
    }

    /**
     * 打开读锁
     */
    @Override
    public void rLock() {
        rLock.lock();
    }

    /**
     * 关闭读锁
     */
    @Override
    public void rUnLock() {
        rLock.unlock();
    }

    /**
     * 获取页面
     */
    @Override
    public Page page() {
        return pg;
    }

    /**
     * 获取Uid
     */
    @Override
    public long getUid() {
        return uid;
    }

    /**
     * 获取修改时暂存的旧数据
     */
    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    /**
     * 获取原始数据
     */
    @Override
    public SubArray getRaw() {
        return raw;
    }
}
