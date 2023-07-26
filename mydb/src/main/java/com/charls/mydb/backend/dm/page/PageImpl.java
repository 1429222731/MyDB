package com.charls.mydb.backend.dm.page;


import com.charls.mydb.backend.dm.pageCache.PageCache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author: charls
 * @Description:TODO
 * @Date: 2023/07/12/ 17:05
 * @Version: 1.0
 */
public class PageImpl implements Page{

    private int pageNumber; // 页面的页号，从1开始
    private byte[] data;    // 这个页实际包含的字节数据
    private boolean dirty;  // 标志这个页面是否是脏页面，缓存驱逐的时候，脏页面需要被写回磁盘

    private Lock lock;

    private PageCache pc;   // 用来方便在拿到 Page 的引用时可以快速对这个页面的缓存进行释放操作。

    /**
     * 构造函数
     * @param pageNumber
     * @param data
     * @param pc
     */
    public PageImpl(int pageNumber, byte[] data, PageCache pc) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.pc=pc;
        lock=new ReentrantLock();
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public void release() {
        pc.release(this);
    }

    @Override
    public void setDirty(boolean dirty) {
        this.dirty=dirty;
    }

    @Override
    public boolean isDirty() {
        return dirty;
    }

    @Override
    public int getPageNumber() {
        return pageNumber;
    }

    @Override
    public byte[] getData() {
        return data;
    }
}
