package com.charls.mydb.backend.tbm;

import com.charls.mydb.backend.dm.DataManager;
import com.charls.mydb.backend.parser.statement.*;
import com.charls.mydb.backend.utils.Parser;
import com.charls.mydb.backend.vm.VersionManager;

/**
 * @Author: charls
 * @Description:TODO
 * @Date: 2023/07/20/ 18:38
 * @Version: 1.0
 */
public interface TableManager {
    BeginRes begin(Begin begin);
    byte[] commit(long xid) throws Exception;
    byte[] abort(long xid);

    byte[] show(long xid);
    byte[] create(long xid, Create create) throws Exception;

    byte[] insert(long xid, Insert insert) throws Exception;
    byte[] read(long xid, Select select) throws Exception;
    byte[] update(long xid, Update update) throws Exception;
    byte[] delete(long xid, Delete delete) throws Exception;

    /**
     * 创建表管理器
     * @param path
     * @param vm
     * @param dm
     * @return
     */
    public static TableManager create(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.create(path);
        booter.update(Parser.long2Byte(0));
        return new TableManagerImpl(vm, dm, booter);
    }

    /**
     * 打开表管理器
     * @param path
     * @param vm
     * @param dm
     * @return
     */
    public static TableManager open(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.open(path);
        return new TableManagerImpl(vm, dm, booter);
    }
}
