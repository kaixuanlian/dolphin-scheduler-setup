package org.apache.dolphinscheduler.remote.utils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 * @Author: Tboy
 */
public class ThreadUtils {

    public static ExecutorService newSingleThreadExecutor(String threadName) {
        return Executors.newSingleThreadExecutor(newThreadFactory(threadName));
    }

    public static ExecutorService newFixedThreadPool(int threadNums, String threadName) {
        return Executors.newFixedThreadPool(threadNums, newThreadFactory(threadName));
    }

    public static ScheduledExecutorService newSingleThreadScheduledExecutor(String threadName) {
        return Executors.newSingleThreadScheduledExecutor(newThreadFactory(threadName));
    }

    public static ScheduledExecutorService newFixedThreadScheduledPool(int threadNums, String threadName) {
        return Executors.newScheduledThreadPool(threadNums, newThreadFactory(threadName));
    }

    public static ThreadFactory newThreadFactory(String threadName) {
        return new ThreadFactoryBuilder()
                .setNameFormat(threadName + "-%d")
                .setDaemon(true)
                .build();
    }

    public static void sleep(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    public static String getProcessName(Class<?> clazz) {
        if ( clazz.isAnonymousClass() )
        {
            return getProcessName(clazz.getEnclosingClass());
        }
        return clazz.getSimpleName();
    }
}
