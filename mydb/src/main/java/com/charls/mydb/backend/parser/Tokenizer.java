package com.charls.mydb.backend.parser;

import com.charls.mydb.common.Error;

/**
 * 对语句进行逐字解析，根据空白符或上述词法规则，将语句切割成多个token。
 * 对外提供了peek()、pop() 方法方便去除 Token进行解析。切割的实现不再赘述
 */
public class Tokenizer {
    private byte[] stat;            // 需要解析的字段
    private int pos;                // 指向token的指针
    private String currentToken;    // 当前Token，如果没有pop()，peek()的时候直接返回currentToken即可
    private boolean flushToken;     // 送出token的一个标记，用于调用pop()的标记，防止peek重复读取token
    private Exception err;

    /**
     * 构造函数
     * @param stat
     */
    public Tokenizer(byte[] stat) {
        this.stat = stat;
        this.pos = 0;
        this.currentToken = "";
        this.flushToken = true;
    }

    /**
     * 送出一个token
     * @return
     * @throws Exception
     */
    public String peek() throws Exception {
        if(err != null) {
            throw err;
        }
        if(flushToken) {
            String token = null;
            try {
                token = next();
            } catch(Exception e) {
                err = e;
                throw e;
            }
            currentToken = token;
            flushToken = false;
        }
        return currentToken;
    }

    // 先peek(),再pop()
    public void pop(){
        flushToken=true;
    }

    /**
     * 返回错误的语句，格式就是在正确雨具与错误雨具之间插入“<<”
     * @return
     */
    public byte[] errStat() {
        byte[] res = new byte[stat.length+3];
        System.arraycopy(stat, 0, res, 0, pos);
        System.arraycopy("<< ".getBytes(), 0, res, pos, 3);
        System.arraycopy(stat, pos, res, pos+3, stat.length-pos);
        return res;
    }

    /**
     * 弹出一个字节，无返回值，需要先试用peekByte()
     */
    private void popByte() {
        pos ++;
        if(pos > stat.length) {
            pos = stat.length;
        }
    }

    private Byte peekByte() {
        if(pos == stat.length) {
            return null;
        }
        return stat[pos];
    }

    private String next() throws Exception {
        if(err != null) {
            throw err;
        }
        return nextMetaState();
    }

    private String nextMetaState() throws Exception {
        while(true) {
            Byte b = peekByte();
            if(b == null) {
                return "";
            }
            if(!isBlank(b)) {
                break;
            }
            popByte();
        }
        byte b = peekByte();
        if(isSymbol(b)) {
            popByte();
            return new String(new byte[]{b});
        } else if(b == '"' || b == '\'') {
            // 返回下一个引用状态
            return nextQuoteState();
        } else if(isAlphaBeta(b) || isDigit(b)) {
            return nextTokenState();
        } else {
            err = Error.InvalidCommandException;
            throw err;
        }
    }

    private String nextTokenState() throws Exception {
        StringBuilder sb = new StringBuilder();
        while(true) {
            Byte b = peekByte();
            if(b == null || !(isAlphaBeta(b) || isDigit(b) || b == '_')) {
                if(b != null && isBlank(b)) {
                    popByte();
                }
                return sb.toString();
            }
            sb.append(new String(new byte[]{b}));
            popByte();
        }
    }

    /**
     * 判断是否是数字
     * @param b
     * @return
     */
    static boolean isDigit(byte b) {
        return (b >= '0' && b <= '9');
    }

    /**
     * 判断是否是字母
     * @param b
     * @return
     */
    static boolean isAlphaBeta(byte b) {
        return ((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z'));
    }

    /**
     * 下一个引用状态
     * @return
     * @throws Exception
     */
    private String nextQuoteState() throws Exception {
        byte quote = peekByte();
        popByte();
        StringBuilder sb = new StringBuilder();
        while(true) {
            Byte b = peekByte();
            if(b == null) {
                err = Error.InvalidCommandException;
                throw err;
            }
            if(b == quote) {
                popByte();
                break;
            }
            sb.append(new String(new byte[]{b}));
            popByte();
        }
        return sb.toString();
    }

    /**
     * 判断是否是这些符号
     * @param b
     * @return
     */
    private boolean isSymbol(byte b) {
        return (b == '>' || b == '<' || b == '=' || b == '*' ||
                b == ',' || b == '(' || b == ')');
    }

    /**
     * 判断是否是换行符（\n）或空字符 或 (\t)   （其中\t代表8个空格，运行到“\t”时，判断当前字符串长度，将当前字符串长度补到8的倍数）
     * @param b
     * @return
     */
    private boolean isBlank(Byte b) {
        return (b == '\n' || b == ' ' || b == '\t');
    }

}
