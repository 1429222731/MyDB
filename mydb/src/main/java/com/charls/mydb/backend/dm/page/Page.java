package com.charls.mydb.backend.dm.page;

/**
 * 页面接口
 */
public interface Page {
    void lock();                    // 加锁
    void unlock();                  // 解锁
    void release();                 // 释放页面缓存
    void setDirty(boolean dirty);   // 设置页面是否为脏页面，修改过的页面就是脏页面
    boolean isDirty();              // 判断页面是否为脏页面
    int getPageNumber();            // 获取页号
    byte[] getData();               // 获取数据页的数据
}
