package com.github.linushp.rocksdb.linkedlist;

import com.github.linushp.rocksdb.utils.RocksFsBase;
import com.github.linushp.rocksdb.utils.RocksSaveable;
import com.github.linushp.rocksdb.utils.StreamingUtils;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class RocksLinkedListNode extends RocksFsBase implements RocksSaveable {
    private byte[] nodeKey;
    private byte[] prevNodeKey;
    private byte[] nextNodeKey;
    private byte[] data;


    public RocksLinkedListNode(RocksDB rocksDB,byte[] nodeKey, RocksLinkedListNode prevNode, byte[] data, RocksLinkedListNode nextNode) {

        super(rocksDB);

        byte[] prevNodeKey = prevNode == null ? null : prevNode.getNodeKey();
        byte[] nextNodeKey = nextNode == null ? null : nextNode.getNodeKey();

        this.nodeKey = nodeKey;
        this.prevNodeKey = prevNodeKey;
        this.data = data;
        this.nextNodeKey = nextNodeKey;
    }


    private RocksLinkedListNode(RocksDB rocksDB,byte[] nodeKey, byte[] prevNodeKey, byte[] data, byte[] nextNodeKey) {
        super(rocksDB);
        this.nodeKey = nodeKey;
        this.prevNodeKey = prevNodeKey;
        this.data = data;
        this.nextNodeKey = nextNodeKey;
    }

    public static RocksLinkedListNode parseObject(RocksDB rocksDB,byte[] bytesData) {
        ByteArrayInputStream stream = new ByteArrayInputStream(bytesData);
        try {
            byte[] nodeKey = StreamingUtils.readTLBytes(stream);
            byte[] prevNodeKey = StreamingUtils.readTLBytes(stream);
            byte[] data = StreamingUtils.readTLBytes(stream);
            byte[] nextNodeKey = StreamingUtils.readTLBytes(stream);
            stream.close();
            return new RocksLinkedListNode(rocksDB,nodeKey, prevNodeKey, data, nextNodeKey);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    public static byte[] toByteArray(RocksLinkedListNode metaData) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            StreamingUtils.writeTLBytes(metaData.nodeKey, stream);
            StreamingUtils.writeTLBytes(metaData.prevNodeKey, stream);
            StreamingUtils.writeTLBytes(metaData.data, stream);
            StreamingUtils.writeTLBytes(metaData.nextNodeKey, stream);
            byte[] bytesData = stream.toByteArray();
            stream.close();
            return bytesData;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    public byte[] getNodeKey() {
        return nodeKey;
    }

    public void setNodeKey(byte[] nodeKey) {
        this.nodeKey = nodeKey;
    }

    public byte[] getPrevNodeKey() {
        return prevNodeKey;
    }

    public void setPrevNodeKey(byte[] prevNodeKey) {
        this.prevNodeKey = prevNodeKey;
    }

    public void setPrevNode(RocksLinkedListNode prevNode) {
        this.prevNodeKey = prevNode == null ? null : prevNode.getNodeKey();
    }

    public byte[] getNextNodeKey() {
        return nextNodeKey;
    }

    public void setNextNodeKey(byte[] nextNodeKey) {
        this.nextNodeKey = nextNodeKey;
    }

    public void setNextNode(RocksLinkedListNode nextNode) {
        this.nextNodeKey = nextNode == null ? null : nextNode.getNodeKey();
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }


    @Override
    public void save(RocksDB rocksDB) throws IOException, RocksDBException {
        byte[] nodeData = toByteArray(this);
        rocksDB.put(this.nodeKey, nodeData);
    }

    @Override
    public void deleteFromRocks(RocksDB rocksDB) throws RocksDBException {
        rocksDB.delete(this.nodeKey);
        this.data = null;
        this.prevNodeKey = null;
        this.nextNodeKey = null;
        this.nodeKey = null;
    }

    public RocksLinkedListNode getNextNode() {
        return getRocksLinkedListNode(this.getNextNodeKey());
    }

    public RocksLinkedListNode getPrevNode() {
        return getRocksLinkedListNode(this.getPrevNodeKey());
    }
}
