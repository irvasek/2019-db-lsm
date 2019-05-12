package ru.mail.polis.vasekha;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class Value implements Comparable<Value> {
    static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);
    private final long timestamp;
    private final boolean isTombstone;
    private final ByteBuffer data;

    Value(final long timestamp, final boolean isTombstone, @NotNull final ByteBuffer data) {
        this.timestamp = timestamp;
        this.isTombstone = isTombstone;
        this.data = data;
    }

    @NotNull
    public static Value of(@NotNull final ByteBuffer data) {
        return new Value(Time.getTimeNanos(), false, data);
    }

    @NotNull
    public static Value remove() {
        return new Value(Time.getTimeNanos(), true, EMPTY_BUFFER);
    }

    public boolean isRemoved() {
        return isTombstone;
    }

    /**
     * Returns the size in bytes that the value will occupy in the file.
     */
    public int getSizeBytes() {
        if (isTombstone) {
            return Long.BYTES;
        } else {
            return Long.BYTES + Integer.BYTES + data.remaining();
        }
    }

    public long getTimestamp() {
        return timestamp;
    }

    @NotNull
    public ByteBuffer getData() {
        return data.asReadOnlyBuffer();
    }

    @Override
    public int compareTo(@NotNull final Value other) {
        return Long.compare(other.timestamp, timestamp);
    }
}
