package com.charls.mydb.client;

import com.charls.mydb.transport.Package;
import com.charls.mydb.transport.Packager;

import java.io.IOException;

/**
 * 实现单次收发动作
 */
public class RoundTripper {
    private Packager packager;

    /**
     * 构造函数
     * @param packager
     */
    public RoundTripper(Packager packager) {
        this.packager = packager;
    }

    /**
     * 单次收发
     * @param pkg
     * @return
     * @throws Exception
     */
    public Package roundTrip(Package pkg) throws Exception {
        packager.send(pkg);
        return packager.receive();
    }

    public void close() throws IOException {
        packager.close();
    }
}
