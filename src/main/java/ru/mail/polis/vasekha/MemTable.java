package ru.mail.polis.vasekha;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
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

    public void clear() {
        table = new TreeMap<>();
        sizeBytes = 0;
    }

    /**
     * Performs flush of the table to the file.
     *
     * @param path the path of the file in which the table will be written
     * @throws IOException if an I/O error occurs
     */
    public void flush(@NotNull final Path path) throws IOException {
        SSTable.writeToFile(path, table.values().iterator());
        clear();
    }
}
