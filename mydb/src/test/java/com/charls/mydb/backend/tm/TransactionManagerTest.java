package com.charls.mydb.backend.tm;

import org.junit.Test;

import java.io.File;
import java.security.SecureRandom;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionManagerTest {

    static Random random = new SecureRandom();

    private int transCnt = 0;
    private int noWorkers = 50;
    private int noWorks = 3000;
    private Lock lock = new ReentrantLock();
    private TransactionManager tmger;
    private Map<Long, Byte> transMap;
    // CountDownLatch是一个同步工具类，它通过一个计数器来实现的,初始值为线程的数量。
    // 每当一个线程完成了自己的任务,计数器的值就相应得减1。当计数器到达0时,表示所有的线程都已执行完毕,然后在等待的线程就可以恢复执行任务。
    private CountDownLatch cdl;

    @Test
    public void testMultiThread() {
        tmger = TransactionManager.create("C:/Users/TDB/tmp/tranmger_test");
        transMap = new ConcurrentHashMap<>();
        cdl = new CountDownLatch(noWorkers);
        for(int i = 0; i < noWorkers; i ++) {
            Runnable r = () -> worker();
            new Thread(r).run();
        }
        try {
            cdl.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        assert new File("C:/Users/TDB/tmp/tranmger_test.xid").delete();
    }

    private void worker() {
        boolean inTrans = false;
        long transXID = 0;
        for(int i = 0; i < noWorks; i ++) {
            int op = Math.abs(random.nextInt(6));
            if(op == 0) {
                lock.lock();
                if(inTrans == false) {
                    long xid = tmger.begin();
                    transMap.put(xid, (byte)0);
                    transCnt ++;
                    transXID = xid;
                    inTrans = true;
                } else {
                    int status = (random.nextInt(Integer.MAX_VALUE) % 2) + 1;
                    switch(status) {
                        case 1:
                            tmger.commit(transXID);
                            break;
                        case 2:
                            tmger.abort(transXID);
                            break;
                    }
                    transMap.put(transXID, (byte)status);
                    inTrans = false;
                }
                lock.unlock();
            } else {
                lock.lock();
                if(transCnt > 0) {
                    long xid = (long)((random.nextInt(Integer.MAX_VALUE) % transCnt) + 1);
                    byte status = transMap.get(xid);
                    boolean ok = false;
                    switch (status) {
                        case 0:
                            ok = tmger.isActive(xid);
                            break;
                        case 1:
                            ok = tmger.isCommitted(xid);
                            break;
                        case 2:
                            ok = tmger.isAborted(xid);
                            break;
                    }
                    assert ok;
                }
                lock.unlock();
            }
        }
        cdl.countDown();
    }
}
