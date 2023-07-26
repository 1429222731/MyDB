package com.charls.mydb.transport;

import com.charls.mydb.common.Error;
import com.google.common.primitives.Bytes;

import java.util.Arrays;

/**
 * 每个 Package 在发送前，由 Encoder 编码为字节数组，在对方收到后同样会由 Encoder 解码成 Package 对象。编码和解码的规则如下：
 * [Flag][data]
 * 如果 flag 为 0，表示发送的是数据，那么 data 即为这份数据本身；
 * 如果 flag 为 1，表示发送的是错误，data 是 Exception.getMessage () 的错误提示信息
 */
public class Encoder {

    /**
     * 将Package编码为字节数组
     * @param pkg
     * @return
     */
    public byte[] encode(Package pkg){
        if (pkg.getErr()!=null){ // 有错误信息
            Exception err=pkg.getErr();
            String msg="Intern server error!";
            if (err.getMessage()!=null){
                msg= err.getMessage();
            }
            return Bytes.concat(new byte[]{1},msg.getBytes());
        }else { // 没有错误信息
            return Bytes.concat(new byte[]{0},pkg.getData());
        }
    }

    /**
     * 将收到的数据解码为Package对象
     * @param data
     * @return
     * @throws Exception
     */
    public Package decode(byte[]data) throws Exception {
        if (data.length<1){
            throw Error.InvalidPkgDataException;
        }

        if (data[0]==0){// 判断是数据
            return new Package(Arrays.copyOfRange(data,1,data.length),null);
        } else if (data[0] == 1) {// 判断是错误
            return new Package(null,new RuntimeException(new String(Arrays.copyOfRange(data,1,data.length))));
        }else {// 其他是无效数据
            throw Error.InvalidPkgDataException;
        }
    }
}
