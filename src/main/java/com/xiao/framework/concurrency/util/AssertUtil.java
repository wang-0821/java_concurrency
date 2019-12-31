package com.xiao.framework.concurrency.util;

/**
 * @author lix wang
 */
public class AssertUtil {
    public static void check(boolean checkValue, String message) {
        if (!checkValue) {
            new RuntimeException(message);
        }
    }
}
