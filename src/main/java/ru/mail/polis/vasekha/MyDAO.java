package ru.mail.polis.vasekha;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Iterator;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;

import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

public final class MyDAO implements DAO {
    private final File folder;
    private final long flushThresholdBytes;
    private final MemTable memTable;
    private final ArrayList<SSTable> ssTables;

    public MyDAO(@NotNull final File folder, final long flushThresholdBytes) throws IOException {
        this.folder = folder;
        this.flushThresholdBytes = flushThresholdBytes;
        memTable = new MemTable();
        ssTables = new ArrayList<>();
        Files.walkFileTree(folder.toPath(), new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                ssTables.add(new SSTable(file));
                return FileVisitResult.CONTINUE;
            }
        });
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {
        final ArrayList<Iterator<Row>> iterators = new ArrayList<>();
        iterators.add(memTable.iterator(from));
        for (final SSTable ssTable : ssTables) {
            iterators.add(ssTable.iterator(from));
        }
        final Iterator<Row> mergeSorted = Iterators.mergeSorted(iterators, Row.comparator);
        final Iterator<Row> collapsed = Iters.collapseEquals(mergeSorted, Row::getKey);
        final Iterator<Row> result = Iterators.filter(collapsed, row -> !row.getValue().isRemoved());
        return Iterators.transform(result, row -> Record.of(row.getKey(), row.getValue().getData()));
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key, value);
        if (memTable.getSizeBytes() >= flushThresholdBytes) {
            flushMemTable();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key);
        if (memTable.getSizeBytes() >= flushThresholdBytes) {
            flushMemTable();
        }
    }

    @Override
    public void close() throws IOException {
        flushMemTable();
    }

    private void flushMemTable() throws IOException {
        final String SUFFIX = ".db";
        final String SUFFIX_TMP = ".txt";
        final String tmpFileName = System.currentTimeMillis() + SUFFIX_TMP;
        memTable.flush(Path.of(folder.getAbsolutePath(), tmpFileName));
        final String finalFileName = System.currentTimeMillis() + SUFFIX;
        Files.move(Path.of(folder.getAbsolutePath(), tmpFileName), Path.of(folder.getAbsolutePath(), finalFileName), StandardCopyOption.ATOMIC_MOVE);
        ssTables.add(new SSTable(Path.of(folder.getAbsolutePath(), finalFileName)));
    }
}
