package com.charls.mydb.backend.dm.pageCache;

import com.charls.mydb.backend.dm.page.Page;
import com.charls.mydb.backend.utils.Panic;
import com.charls.mydb.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * 页面缓存接口
 * 默认提供两个静态方法：
 *      create(String path, long memory)：新建一个数据库文件和数据页面缓存器
 *      open(String path, long memory)：打开一个数据库文件和数据页面缓存器
 */
public interface PageCache {

    public static final int PAGE_SIZE = 1 << 13; // 页面大小 8K

    int newPage(byte[] initData);               // 将数据打包成一个数据页
    Page getPage(int pgno) throws Exception;    // 获取一个数据页
    void close();                               // 关闭数据页缓存器
    void release(Page page);                    // 释放一个数据页缓存

    void truncateByBgno(int maxPgno);           // 删除maxPgno后面的数据页
    int getPageNumber();                        // 获取当前数据库文件的页面总数
    void flushPage(Page pg);                    // 将数据页写入数据源中

    /**
     * 新建一个数据库文件和数据页面缓存器
     * @param path
     * @param memory
     * @return
     */
    public static PageCacheImpl create(String path, long memory) {
        File f = new File(path+PageCacheImpl.DB_SUFFIX);
        try {
            if(!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int)memory/PAGE_SIZE);
    }

    /**
     * 打开一个数据库文件和数据页面缓存器
     * @param path
     * @param memory
     * @return
     */
    public static PageCacheImpl open(String path, long memory) {
        File f = new File(path+PageCacheImpl.DB_SUFFIX);
        if(!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int)memory/PAGE_SIZE);
    }
}
