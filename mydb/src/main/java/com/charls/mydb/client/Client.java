package com.charls.mydb.client;

import com.charls.mydb.transport.Package;
import com.charls.mydb.transport.Packager;

import java.io.IOException;

/**
 * 接收 shell 发过来的sql语句，并打包成Package进行单次收发操作，得到执行结果并返回
 */
public class Client {
    private RoundTripper roundTripper;

    /**
     * 构造函数
     * @param packager
     */
    public Client(Packager packager) {
        this.roundTripper=new RoundTripper(packager);
    }

    /**
     * 接收 shell 发过来的sql语句，并打包成Package进行单次收发操作，得到执行结果并返回
     * @param stat
     * @return
     * @throws Exception
     */
    public byte[] execute(byte[]stat) throws Exception {
        // 初始化Package对象(将sql语句和错误一起打包)
        Package pkg=new Package(stat,null);

        // 进行单次收发(有错误抛出，没有错误就返回数据)
        Package resPkg=roundTripper.roundTrip(pkg);
        if (resPkg.getErr()!=null){
            throw resPkg.getErr();
        }
        return resPkg.getData();
    }

    /**
     * 关闭客户端
     */
    public void close(){
        try {
            roundTripper.close();
        }catch (IOException e){}
    }
}
