package com.charls.mydb.client;

import com.charls.mydb.transport.Encoder;
import com.charls.mydb.transport.Packager;
import com.charls.mydb.transport.Transporter;

import java.io.IOException;
import java.net.Socket;

/**
 * 客户端的启动入口，就是连接上服务器跑一个shell类
 */
public class Launcher {
    public static void main(String[] args) throws IOException {
        Socket socket=new Socket("127.0.0.1",9999);
        Encoder encoder=new Encoder();
        Transporter transporter=new Transporter(socket);
        Packager packager=new Packager(transporter,encoder);

        Client client=new Client(packager);
        Shell shell=new Shell(client);
        shell.run();
    }
}
