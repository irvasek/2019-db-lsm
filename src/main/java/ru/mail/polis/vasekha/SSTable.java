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

public class SSTable implements Table {
    private final int rowsCount;
    private final IntBuffer offsetsBuffer;
    private final ByteBuffer rowsBuffer;

    public SSTable(@NotNull final Path path) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ)) {
            ByteBuffer mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size()).order(ByteOrder.BIG_ENDIAN);
            rowsCount = mappedBuffer.getInt(mappedBuffer.limit() - Integer.BYTES);

            ByteBuffer offsetTmpBuffer = mappedBuffer.duplicate()
                    .position(mappedBuffer.limit() - Integer.BYTES * rowsCount - Integer.BYTES)
                    .limit(mappedBuffer.limit() - Integer.BYTES);
            offsetsBuffer = offsetTmpBuffer.slice().asIntBuffer();

            ByteBuffer rowsTmpBuffer = mappedBuffer.duplicate()
                    .limit(offsetTmpBuffer.position());
            rowsBuffer = rowsTmpBuffer.slice().asReadOnlyBuffer();
        }
    }

    @NotNull
    @Override
    public Iterator<Row> iterator(@NotNull ByteBuffer from) throws IOException {
        return new Iterator<>() {
            private int position = position(from);

            @Override
            public boolean hasNext() {
                return position < rowsCount;
            }

            @Override
            public Row next() {
                return rowAt(position++);
            }
        };
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        throw new UnsupportedOperationException();
    }

    private int position(@NotNull final ByteBuffer key) {
        int left = 0;
        int right = rowsCount - 1;
        while (left <= right) {
            int mid = left + (right - left) / 2;
            int cmp = keyAt(mid).compareTo(key);
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

    private ByteBuffer keyAt(final int position) {
        int offset = offsetsBuffer.get(position);
        int keySize = rowsBuffer.getInt(offset);
        return rowsBuffer.duplicate()
                .position(offset + Integer.BYTES)
                .limit(offset + Integer.BYTES + keySize)
                .slice();
    }

    private Row rowAt(final int position) {
        int offset = offsetsBuffer.get(position);
        int keySize = rowsBuffer.getInt(offset);
        ByteBuffer key = rowsBuffer.duplicate()
                .position(offset + Integer.BYTES)
                .limit(offset + Integer.BYTES + keySize)
                .slice();
        offset += Integer.BYTES + keySize;

        long timestamp = rowsBuffer.position(offset).getLong();
        if (timestamp < 0) {
            return new Row(key, new Value(-timestamp, true, ByteBuffer.allocate(0)));
        }
        int dataSize = rowsBuffer.getInt(offset + Long.BYTES);
        offset += Long.BYTES + Integer.BYTES;
        ByteBuffer data = rowsBuffer.duplicate()
                .position(offset)
                .limit(offset + dataSize)
                .slice();
        return new Row(key, new Value(timestamp, false, data));
    }
}
