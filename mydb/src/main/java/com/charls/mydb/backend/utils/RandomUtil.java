package com.charls.mydb.backend.utils;

import java.security.SecureRandom;
import java.util.Random;

/**
 * @Author: charls
 * @Description:TODO
 * @Date: 2023/07/17/ 15:45
 * @Version: 1.0
 */
public class RandomUtil {
    public static byte[] randomBytes(int length){
        Random r=new SecureRandom();
        byte[] buf=new byte[length];
        r.nextBytes(buf);
        return buf;
    }
}
