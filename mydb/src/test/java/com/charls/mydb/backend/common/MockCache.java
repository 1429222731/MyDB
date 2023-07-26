package com.charls.mydb.backend.common;

/**
 * MockCache 模拟缓存管理器
 */
public class MockCache extends AbstractCache<Long> {

    public MockCache() {
        super(50);
    }

    @Override
    protected Long getForCache(long key) throws Exception {
        return key;
    }

    @Override
    protected void releaseForCache(Long obj) {}
    
}
