package com.github.linushp.rocksdb.linkedlist;

import com.github.linushp.rocksdb.utils.RocksFsBase;
import com.github.linushp.rocksdb.utils.RocksSaveable;
import com.github.linushp.rocksdb.utils.StreamingUtils;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;


public class RocksLinkedListMetaData extends RocksFsBase implements RocksSaveable {


    private int size = 0;

    private byte[] firstNodeKey;

    private byte[] lastNodeKey;

    private byte[] listKey;

    private int modCount = 0;



    private RocksLinkedListMetaData(RocksDB rocksDB,int size, byte[] firstNodeKey, byte[] lastNodeKey, byte[] listKey, int modCount) {
        super(rocksDB);
        this.size = size;
        this.firstNodeKey = firstNodeKey;
        this.lastNodeKey = lastNodeKey;
        this.listKey = listKey;
        this.modCount = modCount;
    }

    static RocksLinkedListMetaData readOrCreateMetaData(RocksDB rocksDB, byte[] listKey) throws Exception {
        RocksLinkedListMetaData metaData = null;
        byte[] bytes = rocksDB.get(listKey);
        if (bytes == null) {
            metaData = new RocksLinkedListMetaData(rocksDB,0,null,null,listKey,0);
            bytes = toByteArray(metaData);
            rocksDB.put(listKey, bytes);
        } else {
            metaData = parseObject(rocksDB,bytes);
        }
        return metaData;
    }


    private static RocksLinkedListMetaData parseObject(RocksDB rocksDB,byte[] bytesData) throws IOException {

        ByteArrayInputStream stream = new ByteArrayInputStream(bytesData);
        int size = StreamingUtils.readInt(stream);
        byte[] first = StreamingUtils.readTLBytes(stream);
        byte[] last = StreamingUtils.readTLBytes(stream);
        byte[] listKey = StreamingUtils.readTLBytes(stream);
        int modCount = StreamingUtils.readInt(stream);
        stream.close();
        return new RocksLinkedListMetaData(rocksDB,size, first, last, listKey, modCount);
    }


    private static byte[] toByteArray(RocksLinkedListMetaData metaData) throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        StreamingUtils.writeInt(metaData.size, stream);
        StreamingUtils.writeTLBytes(metaData.firstNodeKey, stream);
        StreamingUtils.writeTLBytes(metaData.lastNodeKey, stream);
        StreamingUtils.writeTLBytes(metaData.listKey, stream);
        StreamingUtils.writeInt(metaData.modCount, stream);
        byte[] bytesData = stream.toByteArray();
        stream.close();
        return bytesData;
    }


    public byte[] getListKey() {
        return listKey;
    }

    public void setListKey(byte[] listKey) {

        this.listKey = listKey;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {

        this.size = size;
    }

    public byte[] getFirstNodeKey() {
        return firstNodeKey;
    }


    public void setFirstNodeKey(byte[] firstNodeKey) {
        this.firstNodeKey = firstNodeKey;
    }

    public void setFirstNode(RocksLinkedListNode node) {
        if (node == null) {
            this.setFirstNodeKey(null);
        } else {
            this.setFirstNodeKey(node.getNodeKey());
        }
    }

    public void setLastNode(RocksLinkedListNode node) {
        if (node == null) {
            this.setLastNodeKey(null);
        } else {
            this.setLastNodeKey(node.getNodeKey());
        }
    }

    public byte[] getLastNodeKey() {
        return lastNodeKey;
    }

    public void setLastNodeKey(byte[] lastNodeKey) {
        this.lastNodeKey = lastNodeKey;
    }

    public int getModCount() {
        return modCount;
    }

    public void setModCount(int modCount) {

        this.modCount = modCount;
    }

    @Override
    public void save(RocksDB rocksDB) throws IOException, RocksDBException {
        byte[] data = toByteArray(this);
        rocksDB.put(this.listKey, data);
    }

    @Override
    public void deleteFromRocks(RocksDB rocksDB) throws RocksDBException {
        rocksDB.delete(this.listKey);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RocksLinkedListMetaData metaData = (RocksLinkedListMetaData) o;
        return Arrays.equals(listKey, metaData.listKey);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(listKey);
    }


    public Object getListLock() {
        return null;
    }


    public RocksLinkedListNode getLastNode() {
        return getRocksLinkedListNode(this.getLastNodeKey());
    }

    public RocksLinkedListNode getFirstNode() {
        return getRocksLinkedListNode(this.getFirstNodeKey());
    }
}
