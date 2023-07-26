package com.charls.mydb.backend.tm;

import com.charls.mydb.backend.utils.Panic;
import com.charls.mydb.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 提供一些接口供其他模块使用
 * 提供两个静态方法 create 和 open，方便创建一个XID文件并开启事务管理模块，或者从已经存在的XID文件开启事务管理模块
 */
public interface TransactionManager {
    long begin();                       // 开启一个新事务
    void commit(long xid);              // 提交一个事务
    void abort(long xid);               // 取消一个事务
    boolean isActive(long xid);         // 查询一个事务的状态是否是正在进行的状态
    boolean isCommitted(long xid);      // 查询一个事务的状态是否是已提交
    boolean isAborted(long xid);        // 查询一个事务的状态是否是已取消
    void close();                       // 关闭TM


    /**
     * 创建一个新的XID文件并开启事务管理模块
     * @param path
     * @return
     */
    public static TransactionManagerImpl create(String path) {
        // 新建XID文件对象
        File f = new File(path+TransactionManagerImpl.XID_SUFFIX);

        // 一些异常处理
        try {
            if(!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        // 判断读写权限
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        // NIO操作文件的初始化
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        // 写一个空的XID文件头，记录管理了0个事务
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        // 开启事务管理模块
        return new TransactionManagerImpl(raf, fc);
    }

    /**
     * 打开已经存在的XID文件并开启事务管理模块，只是少了一个写入Header的操作
     * @param path
     * @return
     */
    public static TransactionManagerImpl open(String path) {
        File f = new File(path+TransactionManagerImpl.XID_SUFFIX);
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

        return new TransactionManagerImpl(raf, fc);
    }
}
