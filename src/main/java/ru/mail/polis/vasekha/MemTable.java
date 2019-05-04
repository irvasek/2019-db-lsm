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
    private NavigableMap<ByteBuffer, Row> table = new TreeMap<>();
    private long sizeBytes;

    @NotNull
    @Override
    public Iterator<Row> iterator(@NotNull final ByteBuffer from) {
        return table.tailMap(from).values().iterator();
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        final Row current = Row.of(key, value);
        final Row previous = table.put(key, current);
        if (previous == null) {
            sizeBytes += current.getSizeBytes();
        } else {
            sizeBytes += current.getValue().getSizeBytes() - previous.getValue().getSizeBytes();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        final Row current = Row.remove(key);
        final Row previous = table.put(key, current);
        if (previous == null) {
            sizeBytes += current.getSizeBytes();
        } else {
            sizeBytes += current.getValue().getSizeBytes() - previous.getValue().getSizeBytes();
        }
    }

    public long getSizeBytes() {
        return sizeBytes;
    }

    /**
     * performs flush of the table in the file
     *
     * @param path the path of the file in which the table will be written
     * @throws IOException if an I/O error occurs
     */
    public void flush(@NotNull final Path path) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(
                path,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE)) {
            final ByteBuffer offsetsBuffer = ByteBuffer.allocate(table.size() * Integer.BYTES);
            int offset = 0;
            for (final Row row : table.values()) {
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
                    .putInt(table.size())
                    .rewind();
            fileChannel.write(sizeBuffer);
            table = new TreeMap<>();
            sizeBytes = 0;
        }
    }
}
