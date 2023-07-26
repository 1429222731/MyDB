package com.charls.mydb.backend.im;

import com.charls.mydb.backend.common.SubArray;
import com.charls.mydb.backend.dm.DataManager;
import com.charls.mydb.backend.dm.dataItem.DataItem;
import com.charls.mydb.backend.tm.TransactionManagerImpl;
import com.charls.mydb.backend.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * B+ 树索引
 * IM 对上层模块主要提供两种能力：插入索引  和  搜索节点
 */
public class BPlusTree {
    DataManager dm;
    long bootUid;
    Lock bootLock;
    DataItem bootDataItem; // 由于 B+ 树在插入删除时，会动态调整，根节点不是固定节点，于是设置一个 bootDataItem，该 DataItem 中存储了根节点的 UID。

    public static long create(DataManager dm) throws Exception {
        byte[] rawRoot = Node.newNilRootRaw();
        long rootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rawRoot);
        return dm.insert(TransactionManagerImpl.SUPER_XID, Parser.long2Byte(rootUid));
    }

    public static BPlusTree load(long bootUid, DataManager dm) throws Exception {
        DataItem bootDataItem = dm.read(bootUid);
        assert bootDataItem != null;
        BPlusTree t = new BPlusTree();
        t.bootUid = bootUid;
        t.dm = dm;
        t.bootDataItem = bootDataItem;
        t.bootLock = new ReentrantLock();
        return t;
    }

    private long rootUid() {
        bootLock.lock();
        try {
            SubArray sa = bootDataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start, sa.start+8));
        } finally {
            bootLock.unlock();
        }
    }

    private void updateRootUid(long left, long right, long rightKey) throws Exception {
        bootLock.lock();
        try {
            byte[] rootRaw = Node.newRootRaw(left, right, rightKey);
            long newRootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rootRaw);
            bootDataItem.before();
            SubArray diRaw = bootDataItem.data();
            System.arraycopy(Parser.long2Byte(newRootUid), 0, diRaw.raw, diRaw.start, 8);
            bootDataItem.after(TransactionManagerImpl.SUPER_XID);
        } finally {
            bootLock.unlock();
        }
    }

    private long searchLeaf(long nodeUid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        if(isLeaf) {
            return nodeUid;
        } else {
            long next = searchNext(nodeUid, key);
            return searchLeaf(next, key);
        }
    }

    /**
     * serchNext()方法是在索引树上寻找下一个孩子结点，无限循环直到走到叶子结点为止；这个结点没有符合要求的就去下一个兄弟结点找
     */
    private long searchNext(long nodeUid, long key) throws Exception {
        while(true) {
            Node node = Node.loadNode(this, nodeUid);
            Node.SearchNextRes res = node.searchNext(key);
            node.release(); // 释放缓存
            if(res.uid != 0) {
                return res.uid;
            }
            nodeUid = res.siblingUid;
        }
    }

    public List<Long> search(long key) throws Exception {
        return searchRange(key, key);
    }

    public List<Long> searchRange(long leftKey, long rightKey) throws Exception {
        long rootUid = rootUid();
        long leafUid = searchLeaf(rootUid, leftKey);
        List<Long> uids = new ArrayList<>();
        while(true) {
            Node leaf = Node.loadNode(this, leafUid);
            Node.LeafSearchRangeRes res = leaf.leafSearchRange(leftKey, rightKey);
            leaf.release();
            uids.addAll(res.uids);
            if(res.siblingUid == 0) {
                break;
            } else {
                leafUid = res.siblingUid;
            }
        }
        return uids;
    }

    public void insert(long key, long uid) throws Exception {
        long rootUid = rootUid();
        InsertRes res = insert(rootUid, uid, key);
        assert res != null;
        if(res.newNode != 0) {
            updateRootUid(rootUid, res.newNode, res.newKey);
        }
    }

    class InsertRes {
        long newNode, newKey;
    }

    /**
     * 插入结点：
     * 先找到插入位置，也就是要递归到叶子结点为止才会真正插入
     * 期间一直在B+树上面靠serchNext()往下走索引树，serchNext()方法就是寻找下一个孩子结点的uid
     */
    private InsertRes insert(long nodeUid, long uid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        InsertRes res = null;
        if(isLeaf) {
            res = insertAndSplit(nodeUid, uid, key);
        } else {
            long next = searchNext(nodeUid, key);
            InsertRes ir = insert(next, uid, key);
            if(ir.newNode != 0) {
                res = insertAndSplit(nodeUid, ir.newNode, ir.newKey);
            } else {
                res = new InsertRes();
            }
        }
        return res;
    }

    private InsertRes insertAndSplit(long nodeUid, long uid, long key) throws Exception {
        while(true) {
            Node node = Node.loadNode(this, nodeUid);
            Node.InsertAndSplitRes iasr = node.insertAndSplit(uid, key);
            node.release();
            if(iasr.siblingUid != 0) {
                nodeUid = iasr.siblingUid;
            } else {
                InsertRes res = new InsertRes();
                res.newNode = iasr.newSon;
                res.newKey = iasr.newKey;
                return res;
            }
        }
    }

    public void close() {
        bootDataItem.release();
    }
}
