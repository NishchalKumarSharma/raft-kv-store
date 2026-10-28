package com.raft.kv.state;

import java.io.*;
import java.nio.file.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KVStore {

    public enum CommandType {
        PUT, DELETE
    }

    private final Map<String, String> store = new ConcurrentHashMap<>();

    public void put(String key, String value) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        store.put(key, value != null ? value : "");
    }

    public String get(String key) {
        if (key == null) {
            return null;
        }
        return store.get(key);
    }

    public String delete(String key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        return store.remove(key);
    }

    public synchronized String execute(CommandType command, String key, String value) {
        if (command == null) {
            throw new IllegalArgumentException("CommandType cannot be null");
        }

        switch (command) {
            case PUT:
                if (key == null) {
                    throw new IllegalArgumentException("Key cannot be null");
                }
                store.put(key, value != null ? value : "");
                return "SUCCESS";

            case DELETE:
                if (key == null) {
                    throw new IllegalArgumentException("Key cannot be null");
                }
                String removed = store.remove(key);
                return removed != null ? "SUCCESS" : "KEY_NOT_FOUND";

            default:
                throw new UnsupportedOperationException("Unknown Command Type");
        }
    }

    public synchronized Map<String, String> getSnapshot() {
        return new ConcurrentHashMap<>(store);
    }

    public synchronized void restoreSnapshot(Map<String, String> snapshot) {
        store.clear();
        if (snapshot != null) {
            store.putAll(snapshot);
        }
    }

    public void saveSnapshotToDisk(String filePath) throws IOException {
        if (filePath == null || filePath.trim().isEmpty()) {
            throw new IllegalArgumentException("File path cannot be null or empty");
        }

        Map<String, String> snapshot = getSnapshot();
        String tempFilePath = filePath + ".tmp";

        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(tempFilePath))) {
            oos.writeObject(snapshot);
        }

        Files.move(
                Paths.get(tempFilePath),
                Paths.get(filePath),
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE
        );
    }

    public void loadSnapshotFromDisk(String filePath) throws IOException, ClassNotFoundException {
        if (filePath == null || filePath.trim().isEmpty()) {
            throw new IllegalArgumentException("File path cannot be null or empty");
        }

        File file = new File(filePath);
        if (!file.exists() || file.length() == 0) {
            return;
        }

        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file))) {
            @SuppressWarnings("unchecked")
            Map<String, String> loadedData = (Map<String, String>) ois.readObject();
            restoreSnapshot(loadedData);
        }
    }
}