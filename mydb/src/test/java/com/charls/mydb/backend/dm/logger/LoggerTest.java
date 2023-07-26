package com.charls.mydb.backend.dm.logger;

import org.junit.Test;

import java.io.File;

public class LoggerTest {
    @Test
    public void testLogger() {
        Logger lg = Logger.create("C:/Users/TDB/tmp/logger_test");
        lg.log("aaa".getBytes());
        lg.log("bbb".getBytes());
        lg.log("ccc".getBytes());
        lg.log("ddd".getBytes());
        lg.log("eee".getBytes());
        lg.close();

        lg = Logger.open("C:/Users/TDB/tmp/logger_test");
        lg.rewind();

        byte[] log = lg.next();
        assert log != null;
        assert "aaa".equals(new String(log));

        log = lg.next();
        assert log != null;
        assert "bbb".equals(new String(log));

        log = lg.next();
        assert log != null;
        assert "ccc".equals(new String(log));

        log = lg.next();
        assert log != null;
        assert "ddd".equals(new String(log));

        log = lg.next();
        assert log != null;
        assert "eee".equals(new String(log));

        log = lg.next();
        assert log == null;

        lg.close();

        assert new File("C:/Users/TDB/tmp/logger_test.log").delete();
    }
}
