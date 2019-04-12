/*
 * Copyright 2018 (c) Vadim Tsesko <incubos@yandex.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ru.mail.polis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

/**
 * Simple console client to {@link DAO}.
 *
 * @author incubos
 */
public class Client {
    private static final Logger log = LoggerFactory.getLogger(Client.class);
    private static final String DATA = "data";

    public static void main(String[] args) throws IOException {
        final File data = new File(DATA);
        if (!data.exists() && !data.mkdir()) {
            throw new IOException("Can't create directory: " + data);
        }
        if (!data.isDirectory()) {
            throw new IOException("Not directory: " + data);
        }

        log.info("Storing data in {}", data.getAbsolutePath());
        final DAO dao = DAOFactory.create(data);
        final String pkg = dao.getClass().getPackage().toString();
        log.info(
                "Welcome to " + pkg.substring(pkg.lastIndexOf('.') + 1) + " Key-Value DAO!"
                        + "\nSupported commands:"
                        + "\n\tget <key>"
                        + "\n\tput <key> <value>"
                        + "\n\tremove <key>"
                        + "\n\tquit");

        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
            String line;
            while (!"quit".equals(line = reader.readLine())) {
                if (line.isEmpty()) {
                    continue;
                }

                final String[] tokens = line.split(" ");
                final String cmd = tokens[0];
                final ByteBuffer key = ByteBuffer.wrap(tokens[1].getBytes(StandardCharsets.UTF_8));

                switch (cmd) {
                    case "get":
                        try {
                            final ByteBuffer value = dao.iterator(key).next().getValue();
                            final byte[] bytes = new byte[value.remaining()];
                            value.get(bytes);
                            log.info(new String(bytes));
                        } catch (NoSuchElementException e) {
                            log.error("absent");
                        }
                        break;

                    case "put":
                        final ByteBuffer value = ByteBuffer.wrap(tokens[2].getBytes(StandardCharsets.UTF_8));
                        dao.upsert(key, value);
                        break;

                    case "remove":
                        dao.remove(key);
                        break;

                    default:
                        log.error("Unsupported command: {}", cmd);
                }
            }
        }
    }
}
