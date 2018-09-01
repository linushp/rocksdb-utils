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


    protected boolean isByteArrayEqual(byte[] keyBegin, byte[] keyEnd) {
        if (keyBegin == keyEnd) {
            return true;
        }

        if (keyBegin != null && keyEnd != null && keyBegin.length == keyEnd.length) {
            int length = keyBegin.length;
            for (int i = 0; i < length; i++) {
                byte b_1 = keyBegin[i];
                byte b_2 = keyEnd[i];
                if (b_1 != b_2) {
                    return false;
                }
            }
            return true;
        }

        return false;
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
