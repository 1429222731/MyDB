package com.charls.mydb.backend.tm;

import com.charls.mydb.backend.utils.Panic;
import com.charls.mydb.backend.utils.Parser;
import com.charls.mydb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 事务管理模块（TM）
 * XID文件格式：头部用8字节存此XID文件管理的事务总数，后面用1字节存每个事务的状态
 * ｜Header｜ status ｜ status｜ ... ｜status｜
 *  [8Byte]  [1Byte]  [1Byte] [...]  [1Byte]
 *
 * 构造函数需要检查XID文件是否合法，原理是用头部存的事务数量去计算最后一个事务在XID文件中的与起始位置的相对位置，再去对比XID文件的长度
 * 有个超级事务权限SUPER_XID，用于内部控制所有事务的操作。
 * 新建一个新事务的时候使用 ReentrantLock 保证线程安全性
 */
public class TransactionManagerImpl implements TransactionManager{

    // XID文件头长度（使用8字节记录此XID文件管理的事务总数）
    static final int LEN_XID_HEADER_LENGTH = 8;
    // 每个事务的占用长度（使用1字节记录每个事务的状态）
    private static final int XID_FIELD_SIZE = 1;

    // 事务的三种状态（0：正在进行，尚未提交     1：已提交    2：已撤销（回滚））
    private static final byte FIELD_TRAN_ACTIVE   = 0;
    private static final byte FIELD_TRAN_COMMITTED = 1;
    private static final byte FIELD_TRAN_ABORTED  = 2;

    // 超级事务，永远为commited状态
    public static final long SUPER_XID = 0;

    // XID 文件后缀
    static final String XID_SUFFIX = ".xid";


    // 支持“随机访问”的方式，程序快可以直接跳转到文件的任意地方来读写数据。
    private RandomAccessFile file;

    // 直接连接输入输出流的文件通道，将数据直接写入到目标文件中去。
    private FileChannel fc;
    private long xidCounter;
    private Lock counterLock;

    /**
     * 构造函数（需要检查XID文件是否合法）
     * @param file
     * @param fc
     */
    public TransactionManagerImpl(RandomAccessFile file, FileChannel fc) {
        this.file = file;
        this.fc = fc;
        // 初始化wield可重入锁
        counterLock=new ReentrantLock();
        // 检查XID文件是否合法
        checkXIDCounter();
    }

    /**
     * 检查XID文件是否合法
     * 读取XID_FILE_HEADER中xidCounter，根据它计算文件的理论长度，对比实际长度
     * 对于校验没有通过的，会直接通过panic方法来强制停机
     */
    private void checkXIDCounter(){
        long fileLen=0;
        try {
            // 获取文件长度
            fileLen = file.length();
        } catch (IOException e) {
            Panic.panic(Error.BadXIDFileException);
        }
        if (fileLen<LEN_XID_HEADER_LENGTH){
            // 文件头长度不为8字节，就抛出错误异常
            Panic.panic(Error.BadXIDFileException);
        }

        // 读取xid文件中事务的个数
        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try {
            // 读取文件内容的开始位置
            fc.position(0);
            // 读取文件内容，存入到buffer缓冲空间中 =》读取XID文件的文件头
            fc.read(buf);
        }catch (IOException e){
            Panic.panic(e);
        }

        // Parser.parseLong(buffer.array()); 将字节数组包装到新的缓冲区中，并返回缓冲区位置对应位置数据内容
        this.xidCounter=Parser.parseLong(buf.array());
        // 取得最后一个事务在文件中的相对位置，也就是反推xid文件的长度
        long end=getXidPosition(this.xidCounter+1);
        // 文件实际长度和读取到的长度不一致，报错！！
        if (end!=fileLen){
            Panic.panic(Error.BadXIDFileException);
        }
    }

    /**
     * 根据事务xid取得其在XID文件中对应的位置
     * @param xid
     * @return
     */
    private long getXidPosition(long xid) {
        return LEN_XID_HEADER_LENGTH+(xid-1)*XID_FIELD_SIZE;
    }

    /**
     * begin() 方法会开始一个事务，更具体的，首先设置 xidCounter+1 事务的状态为 committed，随后 xidCounter 自增，并更新文件头。
     * @return
     */
    @Override
    public long begin() {
        // 加锁，防止同事开启事务时，事务总数出现错误
        counterLock.lock();
        try {
            // 设置 xidCounter+1 事务的状态为 committed
            long xid=xidCounter+1;
            updateXID(xid,FIELD_TRAN_ACTIVE);
            
            // xidCounter 自增，并更新文件头
            incrXIDCounter();
            return xid;
        }finally {
            counterLock.unlock();
        }
    }

    /**
     * 更新xid事务的状态为status
     * @param xid 事务id
     * @param status 事务需要改变为的状态
     */
    private void updateXID(long xid, byte status) {
        // 获取当前xid事务的相对位置
        long offset = getXidPosition(xid);
        // 事务状态更新为status
        byte[]temp=new byte[XID_FIELD_SIZE];
        temp[0]=status;

        // 使用wrap将字符串转成ByteBuffer
        ByteBuffer buffer = ByteBuffer.wrap(temp);
        // 所有的文件操作在执行后都需要立刻刷入文件中，防止在崩溃后文件丢失数据
        try {
            // 将 buffer 数据写入文件的 offest 位置
            fc.position(offset);
            fc.write(buffer);
        }catch (IOException e){
            Panic.panic(e);
        }
        try {
            /*
             * force(); 强制同步缓存内容到文件中
             * 参数为 boolean 类型，表示是否同步文件的元数据
             * */
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 将xid加一，并更新XID文件中Header的数据
     */
    private void incrXIDCounter() {
        // 增加一个事务
        xidCounter++;
        // 分配缓存空间，将数据存入 buffer
        ByteBuffer buffer = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        // 修改XID Header 更新事务个数
        try {
            fc.position(0);
            fc.write(buffer);
        }catch (IOException e){
            Panic.panic(e);
        }

        try {
            // 注意，这里的所有文件操作，在执行后都需要立刻刷入文件中，防止在崩溃后文件丢失数据，
            // fileChannel 的 force() 方法，强制同步缓存内容到文件中，类似于 BIO 中的 flush() 方法。
            // force 方法的参数是一个布尔，表示是否同步文件的元数据（例如最后修改时间等）。
            fc.force(false);
        }catch (IOException e){
            Panic.panic(e);
        }
    }


    /**
     * 提交XID事务，更新XID文件中对应事务的状态即可
     * @param xid
     */
    @Override
    public void commit(long xid) {
        updateXID(xid,FIELD_TRAN_COMMITTED);
    }

    /**
     * 回滚XID事务，更新XID文件中对应事务的状态即可
     * @param xid
     */
    @Override
    public void abort(long xid) {
        updateXID(xid,FIELD_TRAN_ABORTED);
    }

    /**
     * 查询一个事务的状态是否是正在进行的状态
     * @param xid
     * @return
     */
    @Override
    public boolean isActive(long xid) {
        if (xid==SUPER_XID){
            return false;
        }
        return checkXID(xid,FIELD_TRAN_ACTIVE);
    }

    /**
     * 查询一个事务的状态是否是已提交
     * @param xid
     * @return
     */
    @Override
    public boolean isCommitted(long xid) {
        if (xid==SUPER_XID){
            return true;
        }
        return checkXID(xid,FIELD_TRAN_COMMITTED);
    }

    /**
     * 查询一个事务的状态是否是已取消
     * @param xid
     * @return
     */
    @Override
    public boolean isAborted(long xid) {
        if (xid==SUPER_XID){
            return false;
        }
        return checkXID(xid,FIELD_TRAN_ABORTED);
    }

    /**
     * 检测XID事务是否处于status状态
     * @param xid
     * @param status
     * @return
     */
    private boolean checkXID(long xid, byte status) {
        // 获取当前xid事务的相对位置
        long offset = getXidPosition(xid);
        ByteBuffer buffer = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try {
            fc.position(offset);
            fc.read(buffer);
        }catch (IOException e){
            Panic.panic(e);
        }
        // 校对事务状态是否一致
        return buffer.array()[0]==status;
    }

    /**
     * 关闭TM
     */
    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        }catch (IOException e){
            Panic.panic(e);
        }
    }
}
