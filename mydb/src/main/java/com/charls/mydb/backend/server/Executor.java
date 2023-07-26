package com.charls.mydb.backend.server;

import com.charls.mydb.backend.parser.Parser;
import com.charls.mydb.backend.parser.statement.*;
import com.charls.mydb.backend.tbm.BeginRes;
import com.charls.mydb.backend.tbm.TableManager;
import com.charls.mydb.common.Error;

/**
 *  核心数据处理类，
 *  Executor 调用 Parser 获取到对应语句的结构化信息对象，并根据对象的类型，调用 TBM 的不同方法进行处理
 */
public class Executor {
    private long xid;
    TableManager tbm;

    /**
     * 构造函数
     * @param tbm
     */
    public Executor(TableManager tbm) {
        this.xid = 0; // begin和close操作 需要超级事务进行执行
        this.tbm = tbm;
    }

    /**
     * 不是超级事务，执行abort
     */
    public void close(){
        if (xid!=0){
            System.out.println("Abnormal Abort:"+xid);
            tbm.abort(xid);
        }
    }

    /**
     * 执行begin，commit，abort 三个操作
     * @param sql
     * @return
     */
    public byte[]execute(byte[]sql) throws Exception {
        System.out.println("Execute:"+new String(sql));
        // 获得解析sql语句返回的具体的 Statement 类
        Object stat = Parser.Parse(sql);

        // 判断并执行begin，commit，abort 三个操作
        if (Begin.class.isInstance(stat)){
            if (xid!=0){
                throw Error.NestedTransactionException;
            }
            // 开启表管理和字段管理
            BeginRes r=tbm.begin((Begin) stat);
            xid=r.xid;
            return r.result;
        }else if (Commit.class.isInstance(stat)){
            if (xid!=0){
                throw Error.NoTransactionException;
            }
            byte[]res=tbm.commit(xid);
            xid=0;
            return res;
        } else if (Abort.class.isInstance(stat)) {
            if (xid!=0){
                throw Error.NoTransactionException;
            }
            byte[]res=tbm.abort(xid);
            xid=0;
            return res;
        }else {
            return execute2(stat);
        }
    }

    /**
     * 执行 Show、Create、Select、Read、Insert、Delete、Update
     * @param stat
     * @return
     * @throws Exception
     */
    private byte[] execute2(Object stat) throws Exception {
        boolean tmpTransaction=false;
        Exception e=null;
        if (xid==0){
            tmpTransaction=true;
            BeginRes r = tbm.begin(new Begin());
            xid=r.xid;
        }

        try {
            byte []res=null;

            // 执行 Show、Create、Select、Read、Insert、Delete、Update
            if (Show.class.isInstance(stat)){
                res=tbm.show(xid);
            }else if(Create.class.isInstance(stat)) {
                res = tbm.create(xid, (Create)stat);
            } else if(Select.class.isInstance(stat)) {
                res = tbm.read(xid, (Select)stat);
            } else if(Insert.class.isInstance(stat)) {
                res = tbm.insert(xid, (Insert)stat);
            } else if(Delete.class.isInstance(stat)) {
                res = tbm.delete(xid, (Delete)stat);
            } else if(Update.class.isInstance(stat)) {
                res = tbm.update(xid, (Update)stat);
            }
            return res;
        }catch (Exception e1){
            e=e1;
            throw e;
        }finally {
            if (tmpTransaction){
                // 判断有没有错误，没有错误就commit，有错误就abort
                if (e!=null){
                    tbm.abort(xid);
                }else {
                    tbm.commit(xid);
                }
                xid=0;
            }
        }
    }
}
