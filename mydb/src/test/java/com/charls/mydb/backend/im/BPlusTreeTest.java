package com.charls.mydb.backend.im;

import com.charls.mydb.backend.dm.DataManager;
import com.charls.mydb.backend.dm.pageCache.PageCache;
import com.charls.mydb.backend.tm.MockTransactionManager;
import com.charls.mydb.backend.tm.TransactionManager;
import org.junit.Test;

import java.io.File;
import java.util.List;

public class BPlusTreeTest {
    @Test
    public void testTreeSingle() throws Exception {
        TransactionManager tm = new MockTransactionManager();
        DataManager dm = DataManager.create("C:/Users/TDB/tmp/TestTreeSingle", PageCache.PAGE_SIZE*10, tm);

        long root = BPlusTree.create(dm);
        BPlusTree tree = BPlusTree.load(root, dm);

        int lim = 10000;
        for(int i = lim-1; i >= 0; i --) {
            tree.insert(i, i);
        }

        for(int i = 0; i < lim; i ++) {
            List<Long> uids = tree.search(i);
            assert uids.size() == 1;
            assert uids.get(0) == i;
        }

        assert new File("C:/Users/TDB/tmp/TestTreeSingle.db").delete();
        assert new File("C:/Users/TDB/tmp/TestTreeSingle.log").delete();
    }
}
