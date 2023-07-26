package com.charls.mydb.backend.tbm;

/**
 * 字段索引查询结果的数据结构，存的是B+树的索引范围
 */
public class FieldCalRes {
    public long left;
    public long right;
}
