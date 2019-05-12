package ru.mail.polis.vasekha;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

public final class SSTable implements Table {
    private final Path path;
    private final int rowsCount;
    private final IntBuffer offsetsBuffer;
    private final ByteBuffer rowsBuffer;

    SSTable(@NotNull final Path path) throws IOException {
        this.path = path;
        try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ)) {
            final File file = path.toFile();
            if (file.length() == 0 || file.length() > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Invalid file");
            }
            final ByteBuffer mappedBuffer = fileChannel.map(
                    FileChannel.MapMode.READ_ONLY,
                    0,
                    fileChannel.size()).order(ByteOrder.BIG_ENDIAN);
            this.rowsCount = mappedBuffer.getInt(mappedBuffer.limit() - Integer.BYTES);
            final int position = mappedBuffer.limit() - Integer.BYTES * this.rowsCount - Integer.BYTES;
            if (position < 0 || position > mappedBuffer.limit()) {
                throw new IllegalArgumentException("Invalid file");
            }
            final ByteBuffer offsetsTmpBuffer = mappedBuffer.duplicate()
                    .position(position)
                    .limit(mappedBuffer.limit() - Integer.BYTES);
            this.offsetsBuffer = offsetsTmpBuffer.slice()
                    .asIntBuffer()
                    .asReadOnlyBuffer();
            this.rowsBuffer = mappedBuffer.duplicate()
                    .limit(offsetsTmpBuffer.position())
                    .slice()
                    .asReadOnlyBuffer();
        }
    }

    public Path getPath() {
        return path;
    }

    /**
     * Writes the values to the file.
     * File storage format:
     * Row format in file: key size | key | timestamp | value size | value
     * if value is tombstone
     * key size | key | -timestamp
     * array of offsets that contains positions of rows
     * rows count
     *
     * @param path the path of the file in which the values will be written
     * @throws IOException if an I/O error occurs
     */
    public static void writeToFile(@NotNull final Path path, @NotNull final Collection<Row> values) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(
                path,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE)) {
            final ByteBuffer offsetsBuffer = ByteBuffer.allocate(values.size() * Integer.BYTES);
            int offset = 0;
            for (final Row row : values) {
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
                    .putInt(values.size())
                    .rewind();
            fileChannel.write(sizeBuffer);
        }
    }

    @NotNull
    @Override
    public Iterator<Row> iterator(@NotNull final ByteBuffer from) {
        return new Iterator<>() {
            private int position = position(from);

            @Override
            public boolean hasNext() {
                return position < rowsCount;
            }

            @Override
            public Row next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return rowAt(position++);
            }
        };
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        throw new UnsupportedOperationException();
    }

    private int position(@NotNull final ByteBuffer key) {
        int left = 0;
        int right = rowsCount - 1;
        while (left <= right) {
            final int mid = left + (right - left) / 2;
            final int cmp = keyAt(mid).compareTo(key);
            if (cmp < 0) {
                left = mid + 1;
            } else if (cmp > 0) {
                right = mid - 1;
            } else {
                return mid;
            }
        }
        return left;
    }

    @NotNull
    private ByteBuffer keyAt(final int position) {
        if (position < 0 || position > rowsCount) {
            throw new IllegalArgumentException("Invalid position of key");
        }
        final int offset = offsetsBuffer.get(position);
        final int keySize = rowsBuffer.getInt(offset);
        return rowsBuffer.duplicate()
                .position(offset + Integer.BYTES)
                .limit(offset + Integer.BYTES + keySize)
                .slice()
                .asReadOnlyBuffer();
    }

    @NotNull
    private Row rowAt(final int position) {
        if (position < 0 || position > rowsCount) {
            throw new IllegalArgumentException("Invalid position of row");
        }
        int offset = offsetsBuffer.get(position);
        final int keySize = rowsBuffer.getInt(offset);
        final ByteBuffer key = rowsBuffer.duplicate()
                .position(offset + Integer.BYTES)
                .limit(offset + Integer.BYTES + keySize)
                .slice()
                .asReadOnlyBuffer();
        offset += Integer.BYTES + keySize;

        final long timestamp = rowsBuffer.position(offset).getLong();
        if (timestamp < 0) {
            return new Row(key, new Value(-timestamp, true, Value.EMPTY_BUFFER));
        }
        final int dataSize = rowsBuffer.getInt(offset + Long.BYTES);
        offset += Long.BYTES + Integer.BYTES;
        final ByteBuffer data = rowsBuffer.duplicate()
                .position(offset)
                .limit(offset + dataSize)
                .slice()
                .asReadOnlyBuffer();
        return new Row(key, new Value(timestamp, false, data));
    }
}
