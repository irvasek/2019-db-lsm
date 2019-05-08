package ru.mail.polis.vasekha;

public final class Time {
    private static long lastTime;
    private static int additionalTime;

    private Time() {
    }

    /**
     * Returns time in nanos.
     */
    public static long getTimeNanos() {
        final long currentTime = System.currentTimeMillis();
        if (currentTime != lastTime) {
            additionalTime = 0;
            lastTime = currentTime;
        }
        return currentTime * 1_000_000 + ++additionalTime;
    }
}
