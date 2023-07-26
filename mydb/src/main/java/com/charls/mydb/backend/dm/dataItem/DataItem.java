package com.charls.mydb.backend.dm.dataItem;

import com.charls.mydb.backend.common.SubArray;
import com.charls.mydb.backend.dm.DataManagerImpl;
import com.charls.mydb.backend.dm.page.Page;
import com.charls.mydb.backend.utils.Parser;
import com.charls.mydb.backend.utils.Types;
import com.google.common.primitives.Bytes;

import java.util.Arrays;

/**
 * DataItem 是 DM 层向上层提供的数据抽象。修改数据页全靠传递 DataItem 实现。
 * 上层模块通过地址，向 DM 请求到对应的 DataItem，再获取到其中的数据。
 * 在上层模块试图对 DataItem 进行修改时，需要遵循一定的流程：
 *      在修改之前需要调用 before() 方法，想要撤销修改时，调用 unBefore() 方法，在修改完成后，调用 after() 方法。
 *      整个流程，主要是为了保存前相数据，并及时落日志。DM 会保证对 DataItem 的修改是原子性的。
 *
 *DataItem 中保存的数据，结构如下：
 *      [ValidFlag] [DataSize] [Data]
 *      ValidFlag 占用 1 字节，标识了该 DataItem 是否有效。删除一个 DataItem，只需要简单地将其有效位设置为 0。DataSize 占用 2 字节，标识了后面 Data 的长度。
 */
public interface DataItem {
    SubArray data();        // 通过共享内存的方式返回数据

    void before();          // 修改数据前的方法，打开写锁
    void unBefore();        // 撤销修改，
    void after(long xid);   // 修改数据完成后的方法，记录此事务的修改操作到日志，关闭写锁
    void release();         // 释放 DataItem 缓存

    void lock();            // 打开写锁
    void unlock();          // 关闭写锁
    void rLock();           // 打开读锁
    void rUnLock();         // 关闭读锁

    Page page();            // 获取此 DataItem 所在的数据页
    long getUid();          // 获取 DataItem 的key
    byte[] getOldRaw();     // 获取修改时暂存的旧数据
    SubArray getRaw();

    /**
     * 打包为 dataItem 格式的数据包
     * @param raw
     * @return
     */
    public static byte[] wrapDataItemRaw(byte[] raw) {
        // ValidFlag 占用 1 字节，标识了该 DataItem 是否有效
        byte[] valid = new byte[1];
        // DataSize 占用 2 字节，标识了后面 Data 的长度
        byte[] size = Parser.short2Byte((short)raw.length);
        return Bytes.concat(valid, size, raw);
    }

    /**
     * 从页面的offset处解析出DataItem
     * @param pg
     * @param offset
     * @param dm
     * @return
     */
    public static DataItem parseDataItem(Page pg, short offset, DataManagerImpl dm) {
        // 获取该页数据
        byte[] raw = pg.getData();
        // 读取DataItem的大小
        short size = Parser.parseShort(Arrays.copyOfRange(raw, offset+DataItemImpl.OF_SIZE, offset+DataItemImpl.OF_DATA));
        // 获取整个DataItem的长度
        short length = (short)(size + DataItemImpl.OF_DATA);
        // uid= 页号+偏移量    （将页码和对应页的下标位置拼接）
        long uid = Types.addressToUid(pg.getPageNumber(), offset);
        // 新建共享内存数组，数据位置在 offset ~ offset + length 中
        return new DataItemImpl(new SubArray(raw, offset, offset+length), new byte[length], pg, uid, dm);
    }

    /**
     * 设置 DataItem 的有效性 更改 ValidFlag
     * ValidFlag 占用 1 字节，标识了该 DataItem 是否有效。删除一个 DataItem，只需要简单地将其有效位设置为 0。
     * @param raw
     */
    public static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte)1;
    }
}
