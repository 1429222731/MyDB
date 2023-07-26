package com.charls.mydb.backend.dm.pageIndex;


/**
 * 页面信息数据结构：页号 和 空闲大小
 */
public class PageInfo {
    public int pgno;        // 页号
    public int freeSpace;   // 空闲大小

    public PageInfo(int pgno, int freeSpace) {
        this.pgno = pgno;
        this.freeSpace = freeSpace;
    }
}
