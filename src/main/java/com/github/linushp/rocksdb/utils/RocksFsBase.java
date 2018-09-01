package com.github.linushp.rocksdb.utils;

import com.github.linushp.rocksdb.linkedlist.RocksLinkedListNode;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksFsBase {

    protected RocksDB rocksDB;

    public RocksFsBase(RocksDB rocksDB) {
        this.rocksDB = rocksDB;
    }

    protected RocksLinkedListNode getRocksLinkedListNode(byte[] nodeKey) {
        byte[] data = getRocksData(nodeKey);
        if (data == null) {
            return null;
        }
        return RocksLinkedListNode.parseObject(rocksDB,data);
    }


    protected boolean isEmpty(byte[] byteArray) {
        return byteArray == null || byteArray.length == 0;
    }

    protected byte[] getRocksData(byte[] key) {
        if (isEmpty(key)) {
            return null;
        }
        try {
            return rocksDB.get(key);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return null;
    }

    protected void putRocksData(byte[] key, byte[] data) {
        try {
            rocksDB.put(key, data);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    public void saveRocksNode(RocksSaveable rocksSaveable) {
        if (rocksSaveable != null) {
            try {
                rocksSaveable.save(rocksDB);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    public void deleteRocksNode(RocksSaveable rocksSaveable) {
        if (rocksSaveable != null) {
            try {
                rocksSaveable.deleteFromRocks(rocksDB);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
