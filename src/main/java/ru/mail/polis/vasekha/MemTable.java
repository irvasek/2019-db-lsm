package ru.mail.polis.vasekha;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;

public final class MemTable implements Table {
    private NavigableMap<ByteBuffer, Row> memTable = new TreeMap<>();
    private int memTableSizeBytes = 0;

    @NotNull
    @Override
    public Iterator<Row> iterator(@NotNull ByteBuffer from) throws IOException {
        return memTable.tailMap(from).values().iterator();
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        Row current = Row.of(key, value);
        Row previous = memTable.put(key, current);
        if (previous == null) {
            memTableSizeBytes += current.getSizeBytes();
        } else {
            memTableSizeBytes += current.getValue().getSizeBytes() - previous.getValue().getSizeBytes();
        }
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        Row current = Row.remove(key);
        Row previous = memTable.put(key, current);
        if (previous == null) {
            memTableSizeBytes += current.getSizeBytes();
        } else {
            memTableSizeBytes += current.getValue().getSizeBytes() - previous.getValue().getSizeBytes();
        }
    }

    public long getMemTableSizeBytes() {
        return memTableSizeBytes;
    }

    public void flush(@NotNull final FileChannel fileChannel) throws IOException {
        ByteBuffer offsetsBuffer = ByteBuffer.allocate(memTable.size() * Integer.BYTES);
        int offset = 0;
        for (Row row : memTable.values()) {
            offsetsBuffer.putInt(offset);
            ByteBuffer rowBuffer = ByteBuffer.allocate(row.getSizeBytes());
            ByteBuffer key = row.getKey();
            Value value = row.getValue();
            rowBuffer.putInt(key.remaining())
                    .put(key);
            if (value.isRemoved()) {
                rowBuffer.putLong(-value.getTimestamp());
            } else {
                rowBuffer.putLong(value.getTimestamp())
                        .putInt(value.getData().remaining())
                        .put(value.getData());
            }
            rowBuffer.flip();
            offset += row.getSizeBytes();
            fileChannel.write(rowBuffer);
        }
        offsetsBuffer.flip();
        fileChannel.write(offsetsBuffer);

        ByteBuffer sizeBuffer = ByteBuffer.allocate(Integer.BYTES)
                .putInt(memTable.size())
                .flip();
        fileChannel.write(sizeBuffer);
        memTable = new TreeMap<>();
        memTableSizeBytes = 0;
    }
}
