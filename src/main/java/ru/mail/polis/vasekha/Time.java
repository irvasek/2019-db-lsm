package ru.mail.polis.vasekha;

import java.util.concurrent.atomic.AtomicInteger;

public final class Time {
    private static long lastTime;
    private static AtomicInteger additionalTime = new AtomicInteger();

    private Time() {}

    /**
     * Returns time in nanos.
     */
    public static long getTimeNanos() {
        final long currentTime = System.currentTimeMillis();
        if(currentTime != lastTime){
            additionalTime.set(0);
            lastTime = currentTime;
        }
        return currentTime * 1_000_000 + additionalTime.getAndIncrement();
    }
}
