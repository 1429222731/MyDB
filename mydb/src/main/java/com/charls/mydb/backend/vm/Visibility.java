package com.charls.mydb.backend.vm;

import com.charls.mydb.backend.tm.TransactionManager;

/**
 * MVCC 的代码实现：实现了  读已提交  和  可重复度  两个事务隔离级别
 *
 */
public class Visibility {

    /**
     * 检查是否存在版本跳跃
     *
     * 取出要修改的数据 X 的最新提交版本，并检查该最新版本的创建者对当前事务是否可见
     * @param tm
     * @param t
     * @param e
     * @return
     */
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        long xmax = e.getXmax();
        // 如果事务隔离等级是 读已提交，就允许版本跳跃
        if(t.level == 0) {
            return false;
        } else {
            // 已提交删除当前事务版本，并且这个删除的事务id是在此事务之后发生 或者 是一个未提交的活跃事务操作删除的，就是版本跳跃
            return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
        }
    }

    /**
     * 当前记录版本对事务的可见性
     */
    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        if(t.level == 0) {
            return readCommitted(tm, t, e);
        } else {
            return repeatableRead(tm, t, e);
        }
    }

    /**
     * 读提交
     *
     * 读事务t操作的数据版本，只要没被删除都是可见的；
     * 读其他事务操作的数据版本，要么是已经提交且未删除的，要么是非自己事务进行删除且未提交的，也是对当前事务t可见的
     * @param tm 事务管理器
     * @param t 事务
     * @param e 数据版本链
     * @return 对事务t的可见性
     */
    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;               // 获取事务id
        long xmin = e.getXmin();        // 获取数据版本链最新版本的操作事务id
        long xmax = e.getXmax();        // 获取数据版本链最新版本的删除事务（下一个事务）id
        if(xmin == xid && xmax == 0){
            // 当前数据版本是事务t创建的，并且没有被删除，则对事务t可见
            return true;
        }

        // 由一个已经提交的事务创建
        if(tm.isCommitted(xmin)) {
            // 如果没有被删除，则对事务t可见
            if(xmax == 0) {
                return true;
            }
            // 如果由一个未提交的事务删除当前版本，也对事务t可见
            if(xmax != xid) {
                if(!tm.isCommitted(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 可重复读，多了一个记录活跃事务，简而言之活跃事务操作的数据版本都是不可见的
     *
     * 读取事务t操作的版本只要没被删除都是可见的；
     * 读取其他事务操作过的版本数据，只能读取在本事务开始前就已经提交的事务，并且没有在活跃事务列表里面也没有被删除
     * @param tm 事务管理器
     * @param t 事务
     * @param e 数据版本链
     * @return 对事务t的可见性
     */
    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        // 读取自己操作的版本只要没被删除都是可见的
        if(xmin == xid && xmax == 0) {
            return true;
        }

        // 大范围，只能读取在本事务开始前就已经提交的事务，并且没有在活跃事务列表里面
        if(tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)) {
            // 当前版本还不能被删除
            if(xmax == 0) {
                return true;
            }
            // 删除的事务在本事务之后开始，或者未提交，再或者是活跃事务也是对当前事务可见的
            if(xmax != xid) {
                if(!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }
}
