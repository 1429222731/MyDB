package com.charls.mydb.transport;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.io.*;
import java.net.Socket;

/**
 * 编码之后的信息会通过 Transporter 类，写入输出流发送出去。
 * 为了避免特殊字符造成问题，这里会将数据转成十六进制字符串（Hex String），并为信息末尾加上换行符。
 * 这样在发送和接收数据时，就可以很简单地使用 BufferedReader 和 BufferedWriter 来直接按行读写了。
 */
public class Transporter {
    private Socket socket;
    private BufferedReader reader;
    private BufferedWriter writer;

    /**
     * 构造函数
     * @param socket
     * @throws IOException
     */
    public Transporter(Socket socket) throws IOException {
        this.socket = socket;
        this.reader=new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.writer=new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
    }

    /**
     * 数据的发送
     * @param data
     * @throws IOException
     */
    public void send(byte[]data) throws IOException {
        // 将数据转成十六进制字符串（Hex String），并为信息末尾加上换行符
        String raw=hexEncode(data);
        writer.write(raw);
        writer.flush();
    }

    /**
     * 接收数据
     * @return
     * @throws IOException
     * @throws DecoderException
     */
    public byte[]receive() throws IOException, DecoderException {
        String line=reader.readLine();
        if (line==null){
            // 数据为空，将流关闭
            close();
        }
        return hexDecode(line);
    }

    /**
     * 关闭
     * @throws IOException
     */
    public void close() throws IOException {
        writer.close();
        reader.close();
        socket.close();
    }

    /**
     * 将数据转成十六进制字符串（Hex String），并为信息末尾加上换行符
     * @param buf
     * @return
     */
    private String hexEncode(byte[]buf){
        return Hex.encodeHexString(buf, true)+"\n";
    }

    /**
     * 将十六进制的数据转换成字节数组
     * @param buf
     * @return
     * @throws DecoderException
     */
    private byte[] hexDecode(String buf) throws DecoderException {
        return Hex.decodeHex(buf);
    }
}
