package com.charls.mydb.backend.im;

import com.charls.mydb.backend.common.SubArray;
import com.charls.mydb.backend.dm.dataItem.DataItem;
import com.charls.mydb.backend.tm.TransactionManagerImpl;
import com.charls.mydb.backend.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 二叉树的Node结构如下：
 * 头部信息：
 * [LeafFlag][KeyNumber][SiblingUid]
 * 1byte     2byte      8byte
 * 分支指向信息：
 * [Son0][Key0][Son1][Key1]...[SonN][KeyN]
 * 8byte 8byte
 * 其中 LeafFlag 标记了该节点是否是个叶子节点；KeyNumber 为该节点中 key 的个数；SiblingUid 是其兄弟节点存储在 DM 中的 UID。
 * 后续是穿插的子节点（SonN）和 KeyN。最后的一个 KeyN 始终为 MAX_VALUE，以此方便查找。son值是UID，key值是 where条件搜索的值
 *
 * 这个Node类主要就是那两个方法，serchNext(...) 和 serchRange()；
 * serchNext(...) ：提供给B+树的底层搜索方法，就是一颗多叉搜索树二分搜索算法，目标值小于当前节点值的走左孩子，大于等于的继续for，这个节点都没找到满足要求的就去下一个兄弟节点找
 * serchRange()：提供给B+树的底层范围搜索方法。
 */
public class Node {
    // 节点头部信息偏移量
    static final int IS_LEAF_OFFSET = 0;                        // LeafFlag 起始地址
    static final int NO_KEYS_OFFSET = IS_LEAF_OFFSET + 1;       // KeyNumber 起始地址
    static final int SIBLING_OFFSET = NO_KEYS_OFFSET + 2;       // SiblingUid 起始地址

    // 分支节点指示信息偏移量
    static final int NODE_HEADER_SIZE = SIBLING_OFFSET + 8;     // Son0 起始地址，后续的起始地址靠8byte偏移量计算
    static final int BALANCE_NUMBER = 32;
    static final int NODE_SIZE = NODE_HEADER_SIZE + (2 * 8) * (BALANCE_NUMBER * 2 + 2); // 一个Node结点的空间大小

    // Node 类持有了其 B+ 树结构的引用，DataItem 的引用和 SubArray 的引用，用于方便快速修改数据和释放数据。
    BPlusTree tree;     // B+ 树结构的引用
    DataItem dataItem;  // DM 的数据项引用
    SubArray raw;       // 每个Node结点的内存地址
    long uid;           // DataItem 存储的 uid

    /**
     * 设置Node是否为叶子节点，1是，0不是
     * @param raw
     * @param isLeaf
     */
    static void setRawIsLeaf(SubArray raw, boolean isLeaf) {
        if (isLeaf) {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte) 1;
        } else {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte) 0;
        }
    }

    /**
     * 判断Node是否为叶子节点
     * @param raw
     * @return
     */
    static boolean getRawIfLeaf(SubArray raw) {
        return raw.raw[raw.start + IS_LEAF_OFFSET] == (byte) 1;
    }

    /**
     * 设置Node中的key个数
     * @param raw
     * @param noKeys
     */
    static void setRawNoKeys(SubArray raw, int noKeys) {
        System.arraycopy(Parser.short2Byte((short) noKeys), 0, raw.raw, raw.start + NO_KEYS_OFFSET, 2);
    }

    /**
     * 获取node中的key个数
     * @param raw
     * @return
     */
    static int getRawNoKeys(SubArray raw) {
        return (int) Parser.parseShort(Arrays.copyOfRange(raw.raw, raw.start + NO_KEYS_OFFSET, raw.start + NO_KEYS_OFFSET + 2));
    }

    /**
     * 设置Node的兄弟节点
     * @param raw
     * @param sibling
     */
    static void setRawSibling(SubArray raw, long sibling) {
        System.arraycopy(Parser.long2Byte(sibling), 0, raw.raw, raw.start + SIBLING_OFFSET, 8);
    }

    /**
     * 获取Node的兄弟节点
     * @param raw
     * @return
     */
    static long getRawSibling(SubArray raw) {
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, raw.start + SIBLING_OFFSET, raw.start + SIBLING_OFFSET + 8));
    }

    /**
     * 设置Node的孩子节点
     * @param raw Node
     * @param uid DataItem的id
     * @param kth Node中的Key
     */
    static void setRawKthSon(SubArray raw, long uid, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2);
        System.arraycopy(Parser.long2Byte(uid), 0, raw.raw, offset, 8);
    }

    /**
     * 获取Node的孩子节点的son值
     * @param raw
     * @param kth
     * @return
     */
    static long getRawKthSon(SubArray raw, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2);
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset + 8));
    }

    /**
     * 设置Node的孩子节点key值
     * @param raw
     * @param key
     * @param kth
     */
    static void setRawKthKey(SubArray raw, long key, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2) + 8; // 获取key值
        System.arraycopy(Parser.long2Byte(key), 0, raw.raw, offset, 8);
    }

    /**
     * 获取Node的第kth个孩子节点key值
     * @param raw
     * @param kth
     * @return
     */
    static long getRawKthKey(SubArray raw, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2) + 8;
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset + 8));
    }

    static void copyRawFromKth(SubArray from, SubArray to, int kth) {
        int offset = from.start + NODE_HEADER_SIZE + kth * (8 * 2);
        System.arraycopy(from.raw, offset, to.raw, to.start + NODE_HEADER_SIZE, from.end - offset);
    }

    static void shiftRawKth(SubArray raw, int kth) {
        int begin = raw.start + NODE_HEADER_SIZE + (kth + 1) * (8 * 2);
        int end = raw.start + NODE_SIZE - 1;
        for (int i = end; i >= begin; i--) {
            raw.raw[i] = raw.raw[i - (8 * 2)];
        }
    }

    /**
     * 生成一个非空根节点数据
     * 该根节点的初始两个子节点为 left 和 right
     * @param key 初始键值
     * @return
     */
    static byte[] newRootRaw(long left, long right, long key) {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        setRawIsLeaf(raw, false);             // 设置[LeafFlag]
        setRawNoKeys(raw, 2);                // 设置[KeyNumber]
        setRawSibling(raw, 0);                // 设置[SiblingUid]
        setRawKthSon(raw, left, 0);             // 插入left节点
        setRawKthKey(raw, key, 0);
        setRawKthSon(raw, right, 1);            // 插入right节点
        setRawKthKey(raw, Long.MAX_VALUE, 1);

        return raw.raw;
    }

    /**
     * 生成一个空的根节点数据
     * @return
     */
    static byte[] newNilRootRaw() {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        // 设置节点头部信息
        setRawIsLeaf(raw, true);
        setRawNoKeys(raw, 0);
        setRawSibling(raw, 0);

        return raw.raw;
    }

    /**
     * 从B+树里面获取Node结点信息
     * @param bTree
     * @param uid
     * @return
     * @throws Exception
     */
    static Node loadNode(BPlusTree bTree, long uid) throws Exception {
        DataItem di = bTree.dm.read(uid);
        assert di != null;
        Node n = new Node();
        n.tree = bTree;
        n.dataItem = di;
        n.raw = di.data();
        n.uid = uid;
        return n;
    }

    public void release() {
        dataItem.release();
    }

    /**
     * 判断是否为叶子结点
     * @return
     */
    public boolean isLeaf() {
        dataItem.rLock();
        try {
            return getRawIfLeaf(raw);
        } finally {
            dataItem.rUnLock();
        }
    }

    /**
     * Node 类有两个方法，用于辅助 B+ 树做插入和搜索操作，分别是 searchNext 方法和 leafSearchRange 方法。
     */
    class SearchNextRes {
        long uid;
        long siblingUid;
    }

    /**
     * serchNext()方法是在索引树上寻找下一个孩子结点，无限循环直到走到叶子结点为止；这个结点没有符合要求的就去下一个兄弟结点找
     */
    public SearchNextRes searchNext(long key) {
        dataItem.rLock();
        try {
            SearchNextRes res = new SearchNextRes();
            int noKeys = getRawNoKeys(raw);
            for (int i = 0; i < noKeys; i++) {
                long ik = getRawKthKey(raw, i);
                if (key < ik) {  // 根据排序树的规则，小于的就往左下走就行了
                    res.uid = getRawKthSon(raw, i);
                    res.siblingUid = 0;
                    return res;
                }
            }
            res.uid = 0;
            res.siblingUid = getRawSibling(raw);
            return res;

        } finally {
            dataItem.rUnLock();
        }
    }

    class LeafSearchRangeRes {
        List<Long> uids;
        long siblingUid;
    }

    /**
     * 在当前节点进行范围查找，范围是 [leftKey, rightKey]，
     * 这里约定如果 rightKey 大于等于该节点的最大的 key,则返回兄弟节点的 UID，方便继续搜索下一个节点。
     * @param leftKey
     * @param rightKey
     * @return
     */
    public LeafSearchRangeRes leafSearchRange(long leftKey, long rightKey) {
        dataItem.rLock();
        try {
            int noKeys = getRawNoKeys(raw);
            int kth = 0;
            while (kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if (ik >= leftKey) {
                    break;
                }
                kth++;
            }
            List<Long> uids = new ArrayList<>();
            while (kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if (ik <= rightKey) {
                    uids.add(getRawKthSon(raw, kth));
                    kth++;
                } else {
                    break;
                }
            }
            long siblingUid = 0;
            if (kth == noKeys) {
                siblingUid = getRawSibling(raw);
            }
            LeafSearchRangeRes res = new LeafSearchRangeRes();
            res.uids = uids;
            res.siblingUid = siblingUid;
            return res;
        } finally {
            dataItem.rUnLock();
        }
    }

    class InsertAndSplitRes {
        long siblingUid, newSon, newKey;
    }

    public InsertAndSplitRes insertAndSplit(long uid, long key) throws Exception {
        boolean success = false;
        Exception err = null;
        InsertAndSplitRes res = new InsertAndSplitRes();

        dataItem.before();
        try {
            success = insert(uid, key);
            if (!success) {
                res.siblingUid = getRawSibling(raw);
                return res;
            }
            if (needSplit()) {
                try {
                    SplitRes r = split();
                    res.newSon = r.newSon;
                    res.newKey = r.newKey;
                    return res;
                } catch (Exception e) {
                    err = e;
                    throw e;
                }
            } else {
                return res;
            }
        } finally {
            if (err == null && success) {
                dataItem.after(TransactionManagerImpl.SUPER_XID);
            } else {
                dataItem.unBefore();
            }
        }
    }

    private boolean insert(long uid, long key) {
        int noKeys = getRawNoKeys(raw);
        int kth = 0;
        while (kth < noKeys) {
            long ik = getRawKthKey(raw, kth);
            if (ik < key) {
                kth++;
            } else {
                break;
            }
        }
        if (kth == noKeys && getRawSibling(raw) != 0) {
            return false;
        }

        if (getRawIfLeaf(raw)) {
            shiftRawKth(raw, kth);
            setRawKthKey(raw, key, kth);
            setRawKthSon(raw, uid, kth);
            setRawNoKeys(raw, noKeys + 1);
        } else {
            long kk = getRawKthKey(raw, kth);
            setRawKthKey(raw, key, kth);
            shiftRawKth(raw, kth + 1);
            setRawKthKey(raw, kk, kth + 1);
            setRawKthSon(raw, uid, kth + 1);
            setRawNoKeys(raw, noKeys + 1);
        }
        return true;
    }

    /**
     * 一个节点超过64个孩子就需要分裂了
     * @return
     */
    private boolean needSplit() {
        return BALANCE_NUMBER * 2 == getRawNoKeys(raw);
    }

    class SplitRes {
        long newSon, newKey;
    }

    private SplitRes split() throws Exception {
        SubArray nodeRaw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        setRawIsLeaf(nodeRaw, getRawIfLeaf(raw));
        setRawNoKeys(nodeRaw, BALANCE_NUMBER);
        setRawSibling(nodeRaw, getRawSibling(raw));
        copyRawFromKth(raw, nodeRaw, BALANCE_NUMBER);
        long son = tree.dm.insert(TransactionManagerImpl.SUPER_XID, nodeRaw.raw);
        setRawNoKeys(raw, BALANCE_NUMBER);
        setRawSibling(raw, son);

        SplitRes res = new SplitRes();
        res.newSon = son;
        res.newKey = getRawKthKey(nodeRaw, 0);
        return res;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Is leaf: ").append(getRawIfLeaf(raw)).append("\n");
        int KeyNumber = getRawNoKeys(raw);
        sb.append("KeyNumber: ").append(KeyNumber).append("\n");
        sb.append("sibling: ").append(getRawSibling(raw)).append("\n");
        for (int i = 0; i < KeyNumber; i++) {
            sb.append("son: ").append(getRawKthSon(raw, i)).append(", key: ").append(getRawKthKey(raw, i)).append("\n");
        }
        return sb.toString();
    }
}
