package com.charls.mydb.backend.dm.pageCache;

import com.charls.mydb.backend.common.AbstractCache;
import com.charls.mydb.backend.dm.page.Page;
import com.charls.mydb.backend.dm.page.PageImpl;
import com.charls.mydb.backend.utils.Panic;
import com.charls.mydb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 页面缓存实现类
 * 继承抽象缓存框架AbstractCache，主要重写getForCache 和 releaseForCache方法
 * 实现PageCache接口指定的方法
 */
public class PageCacheImpl extends AbstractCache<Page> implements PageCache {

    private static final int MEM_MIN_LIM = 10;      // 最小缓存数
    public static final String DB_SUFFIX = ".db";   // 文件尾缀

    private RandomAccessFile file;                  // 支持“随机访问”的方式，程序快可以直接跳转到文件的任意地方来读写数据。
    private FileChannel fc;                         // 直接连接输入输出流的文件通道，将数据直接写入到目标文件中去。
    private Lock fileLock;

    private AtomicInteger pageNumbers;              // 记录当前打开的数据库文件有多少页面

    /**
     * 构造函数
     * @param file
     * @param fileChannel
     * @param maxResource
     */
    public PageCacheImpl(RandomAccessFile file, FileChannel fileChannel, int maxResource) {
        super(maxResource);                        // 调用父类的构造函数

        // 缓存空间不能太小
        if(maxResource < MEM_MIN_LIM) {
            Panic.panic(Error.MemTooSmallException);
        }
        long length = 0;                           // 获取DB文件长度
        try {
            length = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.file = file;
        this.fc = fileChannel;
        this.fileLock = new ReentrantLock();
        // 使用JUC下的原子包Int，使用文件长度计算 页面总数
        this.pageNumbers = new AtomicInteger((int)length / PAGE_SIZE);
    }

    /**
     * 当资源不在缓存时的获取行为
     * 根据pageNumber从数据库文件中读取页数据，并包裹成Page
     * @param key 页号
     * @return 数据页
     * @throws Exception
     */
    @Override
    protected Page getForCache(long key) throws Exception {
        int pgno = (int)key;
        long offset = PageCacheImpl.pageOffset(pgno);       // 计算目标数据页在文件中的偏移量

        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);    // 申请一个页面大小的buffer空间
        fileLock.lock();
        try {
            fc.position(offset);                            // 文件指针移动到数据页的偏移位置
            fc.read(buf);                                   // 读取一个页面的数据
        } catch(IOException e) {
            Panic.panic(e);
        }
        fileLock.unlock();
        return new PageImpl(pgno, buf.array(), this);   // 打包成一个数据页
    }

    /**
     * 计算指定页面的偏移量
     * @param pgno 页面编号
     * @return 页面的偏移量
     */
    private static long pageOffset(int pgno) {
        return (pgno-1) * PAGE_SIZE; //  页号从1开始
    }

    /**
     * 当资源被驱逐时的写回行为（只需要根据页面是否是脏页面，来决定是否需要写回文件系统）
     * 调用一个flush()方法将数据页的内容写回数据源
     * @param pg 数据页
     */
    @Override
    protected void releaseForCache(Page pg) {
        if (pg.isDirty()){
            flush(pg);
            pg.setDirty(false);
        }
    }

    /**
     * 将数据打包成一个数据页
     * 调用flush()方法将数据页的内容写入数据源中
     * @param initData 页面数据
     * @return 页号
     */
    public int newPage(byte[] initData) {
        int pgno = pageNumbers.incrementAndGet();           // 使用原子包将页号 +1
        Page pg = new PageImpl(pgno, initData, null);   // 将initData数据包裹成数据页
        flush(pg);                                          // 将数据页中的数据写入数据源
        return pgno;
    }

    /**
     * 获取指定数据页，调用get()方法
     * @param pgno 页号
     * @return 数据页
     * @throws Exception
     */
    @Override
    public Page getPage(int pgno) throws Exception {
        return get((long)pgno);
    }

    /**
     * 关闭数据页缓存器
     */
    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 释放一个数据页缓存
     * @param page
     */
    @Override
    public void release(Page page) {
        release((long) page.getPageNumber());
    }

    /**
     * 删除maxPgno后面的数据页
     * @param maxPgno
     */
    @Override
    public void truncateByBgno(int maxPgno) {
        long size = pageOffset(maxPgno + 1);
        try {
            file.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPgno);
    }

    /**
     * 获取当前数据库文件中的页面总数
     * @return 页面总数
     */
    @Override
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    /**
     * 将数据页写入数据源中
     * @param pg
     */
    @Override
    public void flushPage(Page pg) {
        flush(pg);
    }

    /**
     * 将数据页中的数据写回到数据源文件的规定位置中
     * @param pg 数据页
     */
    private void flush(Page pg) {
        int pgno = pg.getPageNumber();                      // 获取页号
        long offset = pageOffset(pgno);                     // 获取该页面在文件中的偏移量

        // 将数据页中的数据写回数据源文件的指定位置
        fileLock.lock();
        try {
            ByteBuffer buf = ByteBuffer.wrap(pg.getData()); // 从数据页中获取数据内容生成一个buffer
            fc.position(offset);                            // 指针移动到文件的指定位置
            fc.write(buf);                                  // 写回数据源
            fc.force(false);
        } catch(IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
    }
}
