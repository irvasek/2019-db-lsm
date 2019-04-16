package ru.mail.polis.vasekha;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.jetbrains.annotations.NotNull;

import ru.mail.polis.DAO;
import ru.mail.polis.Record;

public class MyDAO implements DAO {
    private final NavigableMap<ByteBuffer, Record> storage = new TreeMap<>();

    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {
        return storage.tailMap(from, true).values().iterator();
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        storage.put(key, Record.of(key, value));
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        storage.remove(key);
    }

    @Override
    public void close() {
        //do nothing
    }
}
