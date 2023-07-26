package com.charls.mydb.backend.vm;

import com.charls.mydb.common.Error;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 维护了一个依赖等待图，以进行死锁检测
 */
public class LockTable {
    private Map<Long, List<Long>> x2u;  // 某个XID已经获得的资源的UID列表
    private Map<Long, Long> u2x;        // UID被某个XID持有
    private Map<Long, List<Long>> wait; // 正在等待UID的XID列表
    private Map<Long, Lock> waitLock;   // 正在等待资源的XID的锁
    private Map<Long, Long> waitU;      // XID正在等待的UID
    private Lock lock;

    private Map<Long, Integer> xidStamp;
    private int stamp;

    /**
     * 构造函数
     */
    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        wait = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * 向依赖等待图中添加一个等待记录
     * 事务xid 阻塞等待 数据项uid，如果会造成死锁则抛出异常
     * @param xid 事务id
     * @param uid 数据项key
     * @return 不需要等待则返回null，否则返回锁对象
     * @throws Exception
     */
    public Lock add(long xid, long uid) throws Exception {
        lock.lock();
        try {
            // 1.判断当前XID需要的UID资源是否已获取（dataItem数据已经被事务xid获取到，不需要等待，返回null）
            if(isInList(x2u, xid, uid)) {
                return null;
            }
            // 2.如果没有获取，判断当前map集合里是否有该键key
            // fasle ==> 占用该 UID
            // containsKey 判断当前 map 集合里是否有该键 key
            if(!u2x.containsKey(uid)) {
                u2x.put(uid, xid);
                // 更新 x2u 集合, UID 资源已获取
                putIntoList(x2u, xid, uid);
                return null;
            }
            // 3. 如果已被其他 XID 占用， 进入等待
            waitU.put(xid, uid);
            putIntoList(wait, xid, uid);
            // 4. 死锁判断 true ==> 回滚，抛出异常
            if(hasDeadLock()) {
                waitU.remove(xid);
                removeFromList(wait, uid, xid);
                throw Error.DeadlockException;
            }
            // 5. 加锁，并等待
            Lock l = new ReentrantLock();
            l.lock();
            waitLock.put(xid, l);
            return l;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 删除XID相关内容
     * @param xid
     */
    public void remove(long xid) {
        lock.lock();
        try {
            // 获取当前 XID 占用的 UID 资源列表
            List<Long> l = x2u.get(xid);
            // 删除
            if(l != null) {
                while(l.size() > 0) {
                    Long uid = l.remove(0);
                    selectNewXID(uid);
                }
            }
            waitU.remove(xid);
            x2u.remove(xid);
            waitLock.remove(xid);

        } finally {
            lock.unlock();
        }
    }

    /**
     * 从等待队列中选择一个xid来占用uid
     * @param uid
     */
    private void selectNewXID(long uid) {
        u2x.remove(uid);
        List<Long> l = wait.get(uid);
        if(l == null) {
            return;
        }
        assert l.size() > 0;

        while(l.size() > 0) {
            long xid = l.remove(0);
            if(!waitLock.containsKey(xid)) {
                continue;
            } else {
                u2x.put(uid, xid);
                Lock lo = waitLock.remove(xid);
                waitU.remove(xid);
                lo.unlock();
                break;
            }
        }

        if(l.size() == 0) {
            wait.remove(uid);
        }
    }

    /**
     * 死锁检测
     * @return
     */
    private boolean hasDeadLock() {
        xidStamp = new HashMap<>();
        stamp = 1;
        for(long xid : x2u.keySet()) {
            // 查询已获得资源的XID的锁标记
            Integer s = xidStamp.get(xid);
            // 锁标记>0  => 有锁且合法
            if(s != null && s > 0) {
                continue;
            }
            // 死锁判断
            stamp++;
            if(dfs(xid)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 死锁判断
     * @param xid
     * @return
     */
    private boolean dfs(long xid) {
        Integer stp = xidStamp.get(xid);
        // 死锁标记判断
        // 获得的锁标记重复 =》 死锁
        if(stp != null && stp == stamp) {
            return true;
        }
        // 获得的锁标记合法 =》 ！死锁
        if(stp != null && stp < stamp) {
            return false;
        }
        // 当前XID锁标记为null，添加锁标记
        xidStamp.put(xid, stamp);
        Long uid = waitU.get(xid);
        // 当前XID没有等待的资源 ==》 ！死锁
        if(uid == null) {
            return false;
        }
        // 等待的资源UID正在被哪个XID占用
        Long x = u2x.get(uid);
        // 如果该XID不存在  报错
        assert x != null;
        // 循环判断该XID 是否死锁
        return dfs(x);
    }

    /**
     * 将集合中下标为 uid0 位置上的 uid1 删除
     * @param listMap
     * @param uid0
     * @param uid1
     */
    private void removeFromList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if(l == null) {
            return;
        }
        Iterator<Long> i = l.iterator();
        while(i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                i.remove();
                break;
            }
        }
        if(l.size() == 0) {
            listMap.remove(uid0);
        }
    }

    /**
     * 将 uid1 放入到集合中 下标为 uid0 的位置
     * @param listMap
     * @param uid0
     * @param uid1
     */
    private void putIntoList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        if(!listMap.containsKey(uid0)) {
            listMap.put(uid0, new ArrayList<>());
        }
        listMap.get(uid0).add(0, uid1);
    }

    /**
     * 判断集合里是否存在 uid1
     * @param listMap
     * @param uid0
     * @param uid1
     * @return
     */
    private boolean isInList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if(l == null) {
            return false;
        }
        Iterator<Long> i = l.iterator();
        while(i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                return true;
            }
        }
        return false;
    }
}
