package com.charls.mydb.backend.utils;

/**
 *调用系统方法强制停机
 */
public class Panic {
    public static void panic(Exception err) {
        err.printStackTrace();
        System.exit(1);
    }
}
