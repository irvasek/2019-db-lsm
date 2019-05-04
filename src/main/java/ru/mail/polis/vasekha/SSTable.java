package ru.mail.polis.vasekha;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NoSuchElementException;

public final class SSTable implements Table {
    private final int rowsCount;
    private final IntBuffer offsetsBuffer;
    private final ByteBuffer rowsBuffer;

    SSTable(@NotNull final Path path) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ)) {
            final ByteBuffer mappedBuffer = fileChannel.map(
                    FileChannel.MapMode.READ_ONLY,
                    0,
                    fileChannel.size()).order(ByteOrder.BIG_ENDIAN);
            rowsCount = mappedBuffer.getInt(mappedBuffer.limit() - Integer.BYTES);
            final ByteBuffer offsetsTmpBuffer = mappedBuffer.duplicate()
                    .position(mappedBuffer.limit() - Integer.BYTES * rowsCount - Integer.BYTES)
                    .limit(mappedBuffer.limit() - Integer.BYTES);
            offsetsBuffer = offsetsTmpBuffer.slice()
                    .asIntBuffer()
                    .asReadOnlyBuffer();
            rowsBuffer = mappedBuffer.duplicate()
                    .limit(offsetsTmpBuffer.position())
                    .slice()
                    .asReadOnlyBuffer();
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
            return new Row(key, new Value(-timestamp, true, ByteBuffer.allocate(0)));
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
