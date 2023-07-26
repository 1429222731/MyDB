package com.charls.mydb.client;

import java.util.Scanner;

/**
 * 读取用户的输入，并调用client.execute()，执行语句，关闭退出等
 */
public class Shell {
    private Client client;

    /**
     * 构造函数
     * @param client
     */
    public Shell(Client client) {
        this.client = client;
    }

    public void run(){
        Scanner sc=new Scanner(System.in);
        try {
            while (true){
                // 读入sql语句
                System.out.println(":>");
                String statStr=sc.nextLine();

                // 如果是exit或者quit，就break掉
                if ("exit".equals(statStr) || "quit".equals(statStr)){
                    break;
                }

                try {
                    // 发送数据给Client,然后执行
                    byte[]res=client.execute(statStr.getBytes());
                    System.out.println(new String(res));
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }finally {
            sc.close();
            client.close();
        }
    }
}

