package com.charls.mydb.backend.common;

/**
 * 共享内存数组
 * 规定这个数组的可使用范围
 */
public class SubArray {
    public byte[]raw;
    public int start;
    public int end;

    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }
}
