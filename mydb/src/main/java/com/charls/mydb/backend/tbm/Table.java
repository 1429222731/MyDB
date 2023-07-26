package com.charls.mydb.backend.tbm;

import com.charls.mydb.backend.parser.statement.*;
import com.charls.mydb.backend.tm.TransactionManagerImpl;
import com.charls.mydb.backend.utils.Panic;
import com.charls.mydb.backend.utils.ParseStringRes;
import com.charls.mydb.backend.utils.Parser;
import com.charls.mydb.common.Error;
import com.google.common.primitives.Bytes;

import java.util.*;

/**
 * Table 维护了表结构
 * 二进制结构如下：
 * [TableName][NextTable]
 * [Field1Uid][Field2Uid]...[FieldNUid]
 *
 * 一个数据库中存在多张表，TBM 使用链表的形式将其组织起来，每一张表都保存一个指向下一张表的 UID。
 */
public class Table {
    TableManager tbm;
    long uid;
    String name;
    byte status;
    long nextUid; // 下一张表的uid
    List<Field> fields = new ArrayList<>();

    /**
     * 构造函数
     */
    public Table(TableManager tbm, long uid) {
        this.tbm = tbm;
        this.uid = uid;
    }

    /**
     * 构造函数
     */
    public Table(TableManager tbm, String tableName, long nextUid) {
        this.tbm = tbm;
        this.name = tableName;
        this.nextUid = nextUid;
    }

    /**
     * 读取一张表
     */
    public static Table loadTable(TableManager tbm, long uid) {
        byte[] raw = null;
        // 先通过VM读取表头UID的Entry
        try {
            raw = ((TableManagerImpl)tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert raw != null;
        Table tb = new Table(tbm, uid);
        return tb.parseSelf(raw);
    }

    /**
     * 创建一张表
     */
    public static Table createTable(TableManager tbm, long nextUid, long xid, Create create) throws Exception {
        Table tb = new Table(tbm, create.tableName, nextUid);
        for(int i = 0; i < create.fieldName.length; i ++) {
            String fieldName = create.fieldName[i];
            String fieldType = create.fieldType[i];
            boolean indexed = false;
            for(int j = 0; j < create.index.length; j ++) {
                if(fieldName.equals(create.index[j])) {
                    indexed = true;
                    break;
                }
            }
            tb.fields.add(Field.createField(tb, xid, fieldName, fieldType, indexed));
        }

        return tb.persistSelf(xid);
    }

    private Table parseSelf(byte[] raw) {
        int position = 0;
        ParseStringRes res = Parser.parseString(raw);
        name = res.str;
        position += res.next;
        nextUid = Parser.parseLong(Arrays.copyOfRange(raw, position, position+8));
        position += 8;

        while(position < raw.length) {
            long uid = Parser.parseLong(Arrays.copyOfRange(raw, position, position+8));
            position += 8;
            fields.add(Field.loadField(this, uid));
        }
        return this;
    }

    private Table persistSelf(long xid) throws Exception {
        byte[] nameRaw = Parser.string2Byte(name);
        byte[] nextRaw = Parser.long2Byte(nextUid);
        byte[] fieldRaw = new byte[0];
        for(Field field : fields) {
            fieldRaw = Bytes.concat(fieldRaw, Parser.long2Byte(field.uid));
        }
        uid = ((TableManagerImpl)tbm).vm.insert(xid, Bytes.concat(nameRaw, nextRaw, fieldRaw));
        return this;
    }

    /**
     * 删除一张表
     */
    public int delete(long xid, Delete delete) throws Exception {
        List<Long> uids = parseWhere(delete.where);
        int count = 0;
        for (Long uid : uids) {
            if(((TableManagerImpl)tbm).vm.delete(xid, uid)) {
                count ++;
            }
        }
        return count;
    }

    /**
     * 更新一张表
     */
    public int update(long xid, Update update) throws Exception {
        List<Long> uids = parseWhere(update.where);
        Field fd = null;
        for (Field f : fields) {
            if(f.fieldName.equals(update.fieldName)) {
                fd = f;
                break;
            }
        }
        if(fd == null) {
            throw Error.FieldNotFoundException;
        }
        Object value = fd.string2Value(update.value);
        int count = 0;
        for (Long uid : uids) {
            byte[] raw = ((TableManagerImpl)tbm).vm.read(xid, uid);
            if(raw == null) {
                continue;
            }

            ((TableManagerImpl)tbm).vm.delete(xid, uid);

            Map<String, Object> entry = parseEntry(raw);
            entry.put(fd.fieldName, value);
            raw = entry2Raw(entry);
            long uuid = ((TableManagerImpl)tbm).vm.insert(xid, raw);

            count ++;

            for (Field field : fields) {
                if(field.isIndexed()) {
                    field.insert(entry.get(field.fieldName), uuid);
                }
            }
        }
        return count;
    }

    /**
     * 读取表中数据
     */
    public String read(long xid, Select read) throws Exception {
        List<Long> uids = parseWhere(read.where);                   // IM查找索引解析Where语句
        StringBuilder sb = new StringBuilder();
        for (Long uid : uids) {
            byte[] raw = ((TableManagerImpl)tbm).vm.read(xid, uid); // 通过VM去读一个Entry记录
            if(raw == null) {
                continue;
            }
            Map<String, Object> entry = parseEntry(raw);
            sb.append(printEntry(entry)).append("\n");
        }
        return sb.toString();
    }

    /**
     * 向表中插入数据
     */
    public void insert(long xid, Insert insert) throws Exception {
        Map<String, Object> entry = string2Entry(insert.values);
        byte[] raw = entry2Raw(entry);
        long uid = ((TableManagerImpl)tbm).vm.insert(xid, raw);
        for (Field field : fields) {
            if(field.isIndexed()) {
                field.insert(entry.get(field.fieldName), uid);
            }
        }
    }

    /**
     * 转换为记录
     */
    private Map<String, Object> string2Entry(String[] values) throws Exception {
        if(values.length != fields.size()) {
            throw Error.InvalidValuesException;
        }
        Map<String, Object> entry = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            Field f = fields.get(i);
            Object v = f.string2Value(values[i]);
            entry.put(f.fieldName, v);
        }
        return entry;
    }

    private List<Long> parseWhere(Where where) throws Exception {
        long l0=0, r0=0, l1=0, r1=0;
        boolean single = false;
        Field fd = null;
        if(where == null) {
            for (Field field : fields) {
                if(field.isIndexed()) {
                    fd = field;
                    break;
                }
            }
            l0 = 0;
            r0 = Long.MAX_VALUE;
            single = true;
        } else {
            for (Field field : fields) {
                if(field.fieldName.equals(where.singleExp1.field)) {
                    if(!field.isIndexed()) {
                        throw Error.FieldNotIndexedException;
                    }
                    fd = field;
                    break;
                }
            }
            if(fd == null) {
                throw Error.FieldNotFoundException;
            }
            CalWhereRes res = calWhere(fd, where);
            l0 = res.l0; r0 = res.r0;
            l1 = res.l1; r1 = res.r1;
            single = res.single;
        }
        List<Long> uids = fd.search(l0, r0);
        if(!single) {
            List<Long> tmp = fd.search(l1, r1);
            uids.addAll(tmp);
        }
        return uids;
    }

    class CalWhereRes {
        long l0, r0, l1, r1;
        boolean single;
    }

    private CalWhereRes calWhere(Field fd, Where where) throws Exception {
        CalWhereRes res = new CalWhereRes();
        switch(where.logicOp) {
            case "":
                res.single = true;
                FieldCalRes r = fd.calExp(where.singleExp1);
                res.l0 = r.left; res.r0 = r.right;
                break;
            case "or":
                res.single = false;
                r = fd.calExp(where.singleExp1);
                res.l0 = r.left; res.r0 = r.right;
                r = fd.calExp(where.singleExp2);
                res.l1 = r.left; res.r1 = r.right;
                break;
            case "and":
                res.single = true;
                r = fd.calExp(where.singleExp1);
                res.l0 = r.left; res.r0 = r.right;
                r = fd.calExp(where.singleExp2);
                res.l1 = r.left; res.r1 = r.right;
                if(res.l1 > res.l0) {
                    res.l0 = res.l1;
                }
                if(res.r1 < res.r0) {
                    res.r0 = res.r1;
                }
                break;
            default:
                throw Error.InvalidLogOpException;
        }
        return res;
    }

    /**
     * 输出记录
     * @param entry
     * @return
     */
    private String printEntry(Map<String, Object> entry) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            sb.append(field.printValue(entry.get(field.fieldName)));
            if(i == fields.size()-1) {
                sb.append("]");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    private Map<String, Object> parseEntry(byte[] raw) {
        int pos = 0;
        Map<String, Object> entry = new HashMap<>();
        for (Field field : fields) {
            Field.ParseValueRes r = field.parserValue(Arrays.copyOfRange(raw, pos, raw.length));
            entry.put(field.fieldName, r.v);
            pos += r.shift;
        }
        return entry;
    }

    private byte[] entry2Raw(Map<String, Object> entry) {
        byte[] raw = new byte[0];
        for (Field field : fields) {
            raw = Bytes.concat(raw, field.value2Raw(entry.get(field.fieldName)));
        }
        return raw;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        sb.append(name).append(": ");
        for(Field field : fields) {
            sb.append(field.toString());
            if(field == fields.get(fields.size()-1)) {
                sb.append("}");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

}
