package com.charls.mydb.backend.server;

import com.charls.mydb.backend.tbm.TableManager;
import com.charls.mydb.transport.Encoder;
import com.charls.mydb.transport.Package;
import com.charls.mydb.transport.Packager;
import com.charls.mydb.transport.Transporter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Server 启动一个 ServerSocket 监听端口，当有请求到来时直接把请求丢给一个新线程处理。
 */
public class Server {
    private int port;
    TableManager tbm;

    /**
     * 构造函数
     * @param port
     * @param tbm
     */
    public Server(int port, TableManager tbm) {
        this.port = port;
        this.tbm = tbm;
    }

    /**
     * 开启服务器
     */
    public void start(){
        ServerSocket ss=null;
        try {
            ss=new ServerSocket(port);
        }catch (IOException e){
            e.printStackTrace();
            return;
        }
        System.out.println("Server listen to port:"+port);
        // 创建一个线程池
        ThreadPoolExecutor threadPoolExecutor=new ThreadPoolExecutor(
                10,                                // 核心线程数
                20,                                           // 池中允许的最大线程数
                1L,                                           // 空闲线程被回收的时间
                TimeUnit.SECONDS,                             // 时间的单位
                new ArrayBlockingQueue<>(100),        // 待执行任务的队列
                new ThreadPoolExecutor.CallerRunsPolicy()     // 拒绝策略
        );
        try {
            while (true){
                Socket socket=ss.accept();
                // 当有请求到来时直接把请求丢给一个新线程处理
                Runnable woeker=new HandleSocket(socket,tbm);
                threadPoolExecutor.execute(woeker);
            }
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            try {
                ss.close();
            }catch (IOException ignopred){}
        }
    }
}

/**
 *  HandleSocket 类实现了 Runnable 接口，
 *  在建立连接后初始化 Packager，循环接收来自客户端的数据并交给 Executor处理，再将处理结果打包发送出去
 */
class HandleSocket implements Runnable{
    private Socket socket;
    private TableManager tbm;

    /**
     * 构造函数
     * @param socket
     * @param tbm
     */
    public HandleSocket(Socket socket, TableManager tbm) {
        this.socket = socket;
        this.tbm = tbm;
    }

    @Override
    public void run() {
        InetSocketAddress address = (InetSocketAddress) socket.getRemoteSocketAddress();
        // 获取到主机地址和端口号
        System.out.println("Establish connection:"+address.getAddress().getHostAddress()+":"+address.getPort());
        Packager packager=null;
        try {
            Transporter t=new Transporter(socket);
            Encoder e=new Encoder();
            // 初始化Packager （Packager 则是 Encoder 和 Transporter 的结合体，直接对外提供 send 和 receive 方法）
            packager=new Packager(t,e);
        }catch (IOException e){
            e.printStackTrace();
            try {
                // 如果有异常，把socket关闭
                socket.close();
            }catch (IOException e1){
                e1.printStackTrace();
            }
            return ;
        }


        // 循环接收来自客户端的数据交给Executor处理
        Executor exe=new Executor(tbm);
        while (true){
            Package pkg=null;
            try {
                // 接受到数据
                pkg=packager.receive();
            }catch (Exception e){
                break;
            }

            // 通过Package对象获取数据
            byte[]sql=pkg.getData();
            byte[]result=null;
            Exception e=null;
            try {
                // 执行sql
                result= exe.execute(sql);
            }catch (Exception e1){
                e=e1;
                e.printStackTrace();
            }

            // 将 Executor 处理结果打包发送出去 (将执行sql的结果封装成package对象，然后发送出去)
            pkg=new Package(result,e);
            try {
                packager.send(pkg);
            }catch (Exception e1){
                e1.printStackTrace();
                break;
            }
        }
        exe.close();
        try {
            packager.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
