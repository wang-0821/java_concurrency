package com.xiao.framework.concurrency.helper;

import com.xiao.framework.concurrency.LinkedTaskQueue;
import com.xiao.framework.concurrency.TaskExecutor;

import java.util.concurrent.TimeUnit;

/**
 * @author lix wang
 */
public class ThreadHelper {
    private static final int POOL_SIZE = 10;
    private static final int KEEP_ALIVE_TIME = 0;
    private static final TimeUnit TIME_UNIT = TimeUnit.SECONDS;

    public static final TaskExecutor POOL = new TaskExecutor(POOL_SIZE, POOL_SIZE,
            0, TIME_UNIT, new LinkedTaskQueue<>());
}
