package com.charls.mydb.backend.dm;

import com.charls.mydb.backend.common.AbstractCache;
import com.charls.mydb.backend.dm.dataItem.DataItem;
import com.charls.mydb.backend.dm.dataItem.DataItemImpl;
import com.charls.mydb.backend.dm.logger.Logger;
import com.charls.mydb.backend.dm.page.Page;
import com.charls.mydb.backend.dm.page.PageOne;
import com.charls.mydb.backend.dm.page.PageX;
import com.charls.mydb.backend.dm.pageCache.PageCache;
import com.charls.mydb.backend.dm.pageIndex.PageIndex;
import com.charls.mydb.backend.dm.pageIndex.PageInfo;
import com.charls.mydb.backend.tm.TransactionManager;
import com.charls.mydb.backend.utils.Panic;
import com.charls.mydb.backend.utils.Types;
import com.charls.mydb.common.Error;

/**
 * DataManager 是 DM 层直接对外提供方法的类，使用 DataItem 进行数据交互，同时也实现了 DataItem 对象的缓存，靠UID查询 DataItem 数据项。
 *  使用分页进行数据的处理，每个页面里有很多个 DataItem 数据项，也就是先找到数据页，再找到 DataItem 数据项进行读写；
 *  uid 是由页号和页内偏移组成的一个 8 字节无符号整数，页号和偏移各占 4 字节，所以通过uid就可以快速定位 DataItem 数据的位置；
 *      DM向上层提供了三个功能：读、插入和修改。
 *      修改是通过读出的 DataItem 然后再插入回去实现的，所以DataManager 只需要提供 read() 和 insert() 方法操作DataItem即可
 *
 *      read(long uid)：根据 UID 从缓存中获取 DataItem，并校验有效位
 *      insert(long xid, byte[] data)：在 pageIndex 中获取一个足以存储插入内容的页面的页号，
 *          获取页面后，首先需要写入插入日志，接着才可以通过 pageX 插入数据，并返回插入位置的偏移。
 *          最后需要将页面信息重新插入 pageIndex
 *
 *
 * DM的所有功能：
 *      1、初始化校验页面1： initPageOne() 和 启动时候进行校验：loadCheckPageOne()
 *      2、读取数据 read(long uid)
 *      3、插入数据 insert(long xid, byte[] data)
 *      4、实现DataItem缓存 重写的两个方法： getForCache(long uid)；releaseForCache(DataItem di)
 *      5、为DataItemImpl.after()提供的记录更新日志方法：logDataItem(long xid, DataItem di)
 *      6、为DataItemImpl.release()提供的释放DataItem缓存方法：releaseDataItem(DataItem di)
 *      7、初始化页面索引：fillPageIndex()
 *      8、关闭DM
 */
public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager{
    TransactionManager tm;
    PageCache pc;
    Logger logger;
    PageIndex pIndex;
    Page pageOne;

    /**
     * 构造函数
     */
    public DataManagerImpl(PageCache pc, Logger logger, TransactionManager tm) {
        super(0);
        this.pc = pc;
        this.logger = logger;
        this.tm = tm;
        this.pIndex = new PageIndex();
    }

    /**
     * 读操作
     *
     * 根据 UID 从缓存中获取 DataItem，并校验有效位
     * @param uid DataItem缓存的key
     * @return DataItem
     */
    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl)super.get(uid); // 从缓存里面取，缓存没有会自动去磁盘取
        if(!di.isValid()) {
            di.release();
            return null;
        }
        return di;
    }

    /**
     * 插入操作
     *
     * 事务xid 插入数据
     * 在 pageIndex 中获取一个足以存储插入内容的页面的页号，
     * 获取页面后，首先需要写入插入日志，接着才可以通过 pageX 插入数据，并返回插入的 DataItem 数据的UID地址。
     * 最后需要将页面信息重新插入 pageIndex
     * @param xid 事务id
     * @param data 数据内容
     * @return UID
     * @throws Exception
     */
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        // 将数据打包为DataItem格式
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if(raw.length > PageX.MAX_FREE_SPACE) {
            throw Error.DataTooLargeException;
        }

        PageInfo pi = null;
        // 在 pageIndex 中获取一个足以存储插入内容的页面的页号，最多尝试五次 (通过索引寻找合适位置放入 dataItem)
        for(int i = 0; i < 5; i ++) {
            // 尝试从页面索引中获取（通过索引查找是否有符合该条 dataItem 大小的空闲页）
            pi = pIndex.select(raw.length);
            if (pi != null) {
                break;
            } else {
                // 获取失败说明已经存在的数据页没有足够的空闲空间插入数据，那么就新建一个数据页
                int newPgno = pc.newPage(PageX.initRaw());
                // 更新页面索引
                pIndex.add(newPgno, PageX.MAX_FREE_SPACE);
            }
        }
        if(pi == null) {
            throw Error.DatabaseBusyException;
        }

        Page pg = null;
        int freeSpace = 0;
        try {
            // 获取插入页号
            pg = pc.getPage(pi.pgno);
            // 先写入插入日志
            byte[] log = Recover.insertLog(xid, pg, raw);
            logger.log(log);

            // 完成页面数据插入，返回在此页面中的插入位置偏移量
            short offset = PageX.insert(pg, raw);

            // 释放此页面缓存
            pg.release();
            // 返回 UID
            return Types.addressToUid(pi.pgno, offset);

        } finally {
            // 最后必须更新pIndex，将取出的pg重新插入pIndex
            if(pg != null) {
                pIndex.add(pi.pgno, PageX.getFreeSpace(pg));
            } else {
                pIndex.add(pi.pgno, freeSpace);
            }
        }
    }

    /**
     * 关闭DM
     */
    @Override
    public void close() {
        super.close();
        logger.close();

        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }

    /**
     * 为xid生成update日志，DataItemImpl.after() 依赖的方法
     * @param xid
     * @param di
     */
    public void logDataItem(long xid, DataItem di) {
        byte[] log = Recover.updateLog(xid, di);
        logger.log(log);
    }

    /**
     * 释放DataItem缓存，DataItemImpl.release() 依赖的方法，其实就是释放DataItem所在页的缓存
     * @param di
     */
    public void releaseDataItem(DataItem di) {
        super.release(di.getUid());
    }

    /**
     * 从数据页缓存中获取一个 DataItem
     * @param uid dataItem的id，页面+偏移量，前32位是页号，后32位是偏移量
     * @return DataItem
     */
    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        int pgno = (int)(uid & ((1L << 32) - 1));
        Page pg = pc.getPage(pgno);
        return DataItem.parseDataItem(pg, offset, this);
    }

    /**
     * DataItem 缓存释放，需要将 DataItem 写回数据源
     * 由于对文件的读写是以页为单位进行的，只需要将 DataItem 所在的页 release 即可
     * @param di
     */
    @Override
    protected void releaseForCache(DataItem di) {
        di.page().release();
    }

    /**
     * 在创建文件时初始化PageOne
     */
    void initPageOne() {
        int pgno = pc.newPage(PageOne.InitRaw());
        assert pgno == 1; // 断言，只有pgno == 1才能继续执行
        try {
            pageOne = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pc.flushPage(pageOne);
    }

    /**
     * 在打开已有文件时时读入PageOne，并验证正确性
     * @return
     */
    boolean loadCheckPageOne() {
        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    /**
     * 初始化pageIndex
     */
    void fillPageIndex() {
        int pageNumber = pc.getPageNumber();
        for(int i = 2; i <= pageNumber; i ++) {
            Page pg = null;
            try {
                pg = pc.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
            pg.release();
        }
    }
}
