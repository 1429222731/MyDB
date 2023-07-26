package com.charls.mydb.backend.dm.logger;

import com.charls.mydb.backend.utils.Panic;
import com.charls.mydb.backend.utils.Parser;
import com.charls.mydb.common.Error;
import com.google.common.primitives.Bytes;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 日志文件读写
 *
 * 日志文件标准格式为：
 * [XChecksum] [Log1] [Log2] ... [LogN] [BadTail]
 * XChecksum 4字节int，为后续所有日志计算的Checksum
 * Log1 ~ LogN 是常规的日志数据
 * BadTail 是在数据库崩溃时，没有来得及写完的日志数据，这个 BadTail 不一定存在。
 *
 * 每条正确日志的格式为：
 * [Size] [Checksum] [Data]
 * Size 4字节int 标识Data长度
 * Checksum 4字节int，单条记录的校验和
 */
public class LoggerImpl implements Logger{

    private static final int SEED = 13331;              // 计算校验和的种子（防止哈希冲突）

    private static final int OF_SIZE = 0;               // 每条记录size的起始偏移量（日志SIZE数据长度）
    private static final int OF_CHECKSUM = OF_SIZE + 4; // 每条记录CheckSum的偏移量（Size占用4字节，所以偏移量+4，也就是日志 Checksum 的所在位置）
    private static final int OF_DATA = OF_CHECKSUM + 4; // 每条记录Data的偏移量（CheckSum占用4字节，所以偏移量+4，也就是日志 data[] 数据所在位置）

    public static final String LOG_SUFFIX = ".log";     // 日志文件的后缀

    private RandomAccessFile file;                      // 支持“随机访问”的方式，程序快可以直接跳转到文件的任意地方来读写数据。
    private FileChannel fc;                             // 直接连接输入输出流的文件通道，将数据直接写入到目标文件中去。
    private Lock lock;

    private long position;                              // 当前日志指针的位置
    private long fileSize;                              // 日志文件的大小，初始化时记录，log操作不更新
    private int xChecksum;                              // 日志文件的总 校验和

    /**
     * 构造函数
     * @param raf
     * @param fc
     */
    LoggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        lock = new ReentrantLock();
    }

    /**
     * 构造函数
     * @param raf
     * @param fc
     * @param xChecksum
     */
    LoggerImpl(RandomAccessFile raf, FileChannel fc, int xChecksum) {
        this.file = raf;
        this.fc = fc;
        this.xChecksum = xChecksum;                    // 和上一个构造函数相比  多了一个总校验和
        lock = new ReentrantLock();
    }

    /**
     * 日志初始化操作，完成日志文件长度的读取、总校验和的读取  和 BadTail的移除
     */
    void init(){
        // 1.获取文件大小、判断文件是否合法
        long size=0;
        try {
            size=file.length();
        }catch (IOException e){
            Panic.panic(e);
        }
        if (size<4){
            Panic.panic(Error.BadLogFileException);
        }

        // 2.读取日志文件的大小 和 总校验xChecksum
        ByteBuffer raw = ByteBuffer.allocate(4);
        try {
            fc.position(0);
            fc.read(raw);
        }catch (IOException e){
            Panic.panic(e);
        }
        int xChecksum = Parser.parseInt(raw.array());
        this.fileSize=size;  // 文件长度
        this.xChecksum=xChecksum;

        // 3.检查并移除可能存在的 badTail
        checkAndRemoveTail();
    }

    /**
     * 检查并移除 badTail
     */
    private void checkAndRemoveTail(){
        // 将指针指向第一条日志记录
        rewind();

        int xCheck = 0;
        while(true) {
            // 循环遍历获取每一条日志记录
            byte[] log = internNext();
            if(log == null) {
                break;
            }
            // 对每条记录都进行计算校验和累加值，就是xChecksum
            xCheck = calChecksum(xCheck, log);
        }
        // 校验和不相等
        if(xCheck != xChecksum) {
            Panic.panic(Error.BadLogFileException);
        }

        // 校验和相等
        try {
            truncate(position);// 截断文件到正常日志的末尾，因为前面计算校验和移动了指针，此时的position指向的就是badTail的起始位置
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            file.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }
        // 将position指针指向第一条日志记录，还原指针操作
        rewind();
    }

    /**
     * 单条日志的校验和
     * 其实就是通过一个指定的种子实现的，对日志的每个字节乘一个SEED再累加，就能得到单条日志文件的校验和了
     * @param xCheck 校验和初始值，一般是0
     * @param log 需要计算校验和的单条日志文件
     * @return 单条日志的校验和
     */
    private int calChecksum(int xCheck,byte[]log){
        for (byte b:log){
            xCheck=xCheck*SEED+b;
        }
        return xCheck;
    }

    /**
     * 写入一条日志记录
     *
     * 将数据包裹成日志格式，写入文件后，再更新文件的校验和，更新校验和时，会刷新缓冲区，保证内容写入磁盘
     * @param data 日志数据
     */
    @Override
    public void log(byte[] data) {
        byte[] log = wrapLog(data); // 将数据打包成正确的日志格式
        ByteBuffer buf = ByteBuffer.wrap(log);
        lock.lock();
        try {
            fc.position(fc.size());// 指针移动到文件末尾
            fc.write(buf);         // 将记录写入文件
        } catch(IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
        updateXChecksum(log);      // 更新总校验和
    }

    /**
     * 更新日志文件的总校验和
     * @param log
     */
    private void updateXChecksum(byte[] log) {
        this.xChecksum = calChecksum(this.xChecksum, log);
        try {
            fc.position(0);
            fc.write(ByteBuffer.wrap(Parser.int2Byte(xChecksum)));
            fc.force(false);
        } catch(IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 打包日志文件成一个二进制字节数组
     * @param data
     * @return
     */
    private byte[] wrapLog(byte[] data) {
        byte[] checksum = Parser.int2Byte(calChecksum(0, data)); // 计算单条日志的校验和
        byte[] size = Parser.int2Byte(data.length);                     // 计算data的大小
        return Bytes.concat(size, checksum, data);                      // 打包成标准的单条日志格式，并返回
    }

    /**
     * 截断日志文件，删除x偏移量后面的文件数据
     * @param x 截断位置
     * @throws Exception
     */
    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fc.truncate(x);
        }finally {
            lock.unlock();
        }
    }

    /**
     * 迭代器方式。从文件中读取下一条日志，并将其中的 Data解析出来并返回
     * 实现主要依靠 internNext()
     * @return
     */
    @Override
    public byte[] next() {
        lock.lock();
        try {
            // 获取一条完整日志
            byte []log=internNext();
            if (log==null){
                return null;
            }
            return Arrays.copyOfRange(log,OF_DATA,log.length);// 解析出日志中的DATA数据并返回
        }finally {
            lock.unlock();
        }
    }

    /**
     * 迭代器，获取下一条完整日志记录
     * @return 一条格式完整的完整的日志记录 [Size] [Checksum] [Data]
     */
    private byte[] internNext() {
        // 判断文件大小是否合法
        if (position+OF_DATA>=fileSize){
            // 文件指针越界，也就是没有下一条日志了
            return null;
        }

        // 读取单条日志的size
        ByteBuffer tmp = ByteBuffer.allocate(4);
        try {
            fc.position(position);
            fc.read(tmp);
        }catch (IOException e){
            Panic.panic(e);
        }
        int size = Parser.parseInt(tmp.array());
        // 判断position是否已经读到了文件末尾
        if (position+size+OF_DATA>fileSize){
            return null;
        }

        // 读取 size+checkSum+data
        ByteBuffer buf = ByteBuffer.allocate(OF_DATA+size);
        try {
            fc.position(position);
            fc.read(buf);
        }catch (IOException e){
            Panic.panic(e);
        }
        // 将日志内容打包成二进制数组
        byte []log=buf.array();

        // 校验 单条日志的 checksum
        int checkSum1 = calChecksum(0, Arrays.copyOfRange(log, OF_DATA, log.length));// 使用data计算checksum
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));// 获取日志中记录的checksum
        if (checkSum1!=checkSum2){
            return null;
        }

        // 文件指针指向下一条日志记录
        position+=log.length;

        return log;
    }

    /**
     * 将文件指针指向第一条记录的起始位置
     */
    @Override
    public void rewind() {
        position=4;
    }

    /**
     * 关闭日志管理器
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
