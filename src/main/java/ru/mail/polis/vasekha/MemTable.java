package ru.mail.polis.vasekha;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;

public final class MemTable implements Table {
    private NavigableMap<ByteBuffer, Row> memTable = new TreeMap<>();
    private long sizeBytes = 0;

    @NotNull
    @Override
    public Iterator<Row> iterator(@NotNull ByteBuffer from) {
        return memTable.tailMap(from).values().iterator();
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) {
        final Row current = Row.of(key, value);
        final Row previous = memTable.put(key, current);
        if (previous == null) {
            sizeBytes += current.getSizeBytes();
        } else {
            sizeBytes += current.getValue().getSizeBytes() - previous.getValue().getSizeBytes();
        }
    }

    @Override
    public void remove(@NotNull ByteBuffer key) {
        final Row current = Row.remove(key);
        final Row previous = memTable.put(key, current);
        if (previous == null) {
            sizeBytes += current.getSizeBytes();
        } else {
            sizeBytes += current.getValue().getSizeBytes() - previous.getValue().getSizeBytes();
        }
    }

    public long getSizeBytes() {
        return sizeBytes;
    }

    public void flush(@NotNull final Path path) throws IOException {
        try (final FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            final ByteBuffer offsetsBuffer = ByteBuffer.allocate(memTable.size() * Integer.BYTES);
            int offset = 0;
            for (final Row row : memTable.values()) {
                offsetsBuffer.putInt(offset);
                final ByteBuffer rowBuffer = ByteBuffer.allocate(row.getSizeBytes());
                final ByteBuffer key = row.getKey();
                final Value value = row.getValue();
                rowBuffer.putInt(key.remaining())
                        .put(key);
                if (value.isRemoved()) {
                    rowBuffer.putLong(-value.getTimestamp());
                } else {
                    rowBuffer.putLong(value.getTimestamp())
                            .putInt(value.getData().remaining())
                            .put(value.getData());
                }
                rowBuffer.rewind();
                fileChannel.write(rowBuffer);
                offset += row.getSizeBytes();
            }
            offsetsBuffer.rewind();
            fileChannel.write(offsetsBuffer);
            final ByteBuffer sizeBuffer = ByteBuffer.allocate(Integer.BYTES)
                    .putInt(memTable.size())
                    .rewind();
            fileChannel.write(sizeBuffer);
            memTable = new TreeMap<>();
            sizeBytes = 0;
        }
    }
}
