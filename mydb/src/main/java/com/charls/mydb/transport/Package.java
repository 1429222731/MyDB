package com.charls.mydb.transport;

/**
 * 将sql语句和错误一起打包
 */
public class Package {
    byte []data;  // sql语句
    Exception err;

    /**
     * 构造函数
     * @param data
     * @param err
     */
    public Package(byte[] data, Exception err) {
        this.data = data;
        this.err = err;
    }

    public byte[] getData() {
        return data;
    }

    public Exception getErr() {
        return err;
    }
}
