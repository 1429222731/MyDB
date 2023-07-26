package com.charls.mydb.backend.dm.page;

import com.charls.mydb.backend.dm.pageCache.PageCache;
import com.charls.mydb.backend.utils.RandomUtil;

import java.util.Arrays;

/**
 * 特殊管理第一页
 * ValidCheck 校验页面，唯一作用就是校验数据库是否正常关闭
 * db启动时给100~107字节处填入一个随机字节，db关闭时将其拷贝到108~115字节
 * 数据库在每次启动时，就会检查第一页 两处的字节是否相同，以此来判断上一次是否正常关闭。如果是异常关闭，就需要执行数据的恢复流程。
 */
public class PageOne {
    private static final int OF_VC=100;
    private static final int LEN_VC=8;

    /**
     * 初始化一个页面
     * @return 一个设置了100~107字节处随机数的特殊页面1
     */
    public static byte[] InitRaw(){
        byte[] raw = new byte[PageCache.PAGE_SIZE];   // 新建一个数据页大小的字节数组
        setVcOpen(raw);                               // 调用setVcOpen(byte[] raw) 在100~107字节处填入一个8位的随机数
        return raw;
    }

    /**
     * 启动时设置初始字节
     * @param pg
     */
    public static void setVcOpen(Page pg){
        // 设置为脏页
        pg.setDirty(true);
        // 给100~107字节处设置随机字节
        setVcOpen(pg.getData());
    }

    /**
     * 给100~107字节处设置随机字节（数据库启动的时候，在raw文件指定位置填入一个指定大小的随机数）
     * @param raw
     */
    private static void setVcOpen(byte[] raw) {
        // 原数组   原数组开始   目标数组   目标数组开始   截取的长度
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OF_VC, LEN_VC);
    }

    /**
     * 正常关闭数据库,会拷贝开机时生成的随机字符串
     * @param pg
     */
    public static void setVcClose(Page pg) {
        // 设置脏页
        pg.setDirty(true);
        // 关闭时拷贝 108-115字节
        setVcClose(pg.getData());
    }

    /**
     * 关闭时拷贝 108-115字节（数据库关闭的时候，把raw文件中100~107字节处的内容拷贝到108~115字节处）
     * @param raw
     */
    private static void setVcClose(byte[] raw) {
        // 原数组   原数组开始   目标数组   目标数组开始   截取的长度
        System.arraycopy(raw, OF_VC, raw, OF_VC+LEN_VC, LEN_VC);
    }

    /**
     * 校验字节(检查上述两段随机字符串是否一样,就可以判断是否是正常关闭还是异常关闭)
     * @param pg
     * @return
     */
    public static boolean checkVc(Page pg) {
        return checkVc(pg.getData());
    }

    /**
     * 验证raw文件中100~107字节处的内容和108~115字节处内容是否一致
     * @param raw
     * @return
     */
    private static boolean checkVc(byte[] raw) {
        return Arrays.equals(Arrays.copyOfRange(raw, OF_VC, OF_VC+LEN_VC), Arrays.copyOfRange(raw, OF_VC+LEN_VC, OF_VC+2*LEN_VC));
    }

}
