package com.charls.mydb.transport;

import java.io.IOException;

/**
 * Packager 则是 Encoder 和 Transporter 的结合体，直接对外提供 send 和 receive 方法
 */
public class Packager {
    private Transporter transporter;
    private Encoder encode;

    /**
     * 构造函数
     * @param transporter
     * @param encode
     */
    public Packager(Transporter transporter, Encoder encode) {
        this.transporter = transporter;
        this.encode = encode;
    }

    /**
     * 通过Transporter类 发送 数据
     * @param pkg
     * @throws IOException
     */
    public void send(Package pkg) throws IOException {
        // 将Package对象编码成要发送的字节数据
        byte[]data=encode.encode(pkg);
        // 编码之后的信息会通过 Transporter 类，写入输出流发送出去。
        transporter.send(data);
    }

    /**
     * 通过Transporter类 接收 数据
     * @return
     * @throws Exception
     */
    public Package receive() throws Exception {
        // 通过Transporter 类接收数据
        byte[]data= transporter.receive();
        // 然后将数据解码
        return encode.decode(data);
    }

    /**
     * 通过Transporter类 关闭流
     * @throws IOException
     */
    public void close() throws IOException {
        transporter.close();
    }
}
