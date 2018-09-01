package com.github.linushp.rocksdb.utils;

import org.rocksdb.RocksDB;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class NodeKeyManager {
    private static final int MinCurrent = 100;
    private RocksDB rocksDB;
    private byte[] mask;
    private int current;


    public static NodeKeyManager getInstance(RocksDB rocksDB, byte[] mask) throws Exception {
        byte[] selfNodeKey = getSelfNodeKey(mask);
        byte[] bytesData = rocksDB.get(selfNodeKey);
        if (bytesData == null) {
            return new NodeKeyManager(rocksDB, mask, MinCurrent);
        }
        int current = StreamingUtils.readInt(bytesData);
        return new NodeKeyManager(rocksDB, mask, current);
    }


    private NodeKeyManager(RocksDB rocksDB, byte[] mask, int current) {
        this.rocksDB = rocksDB;
        this.mask = mask;
        this.current = current;
    }

    private static byte[] getSelfNodeKey(byte[] mask) throws IOException {
        return getNodeKey(mask, 0);
    }
    public byte[] getMetaDataNodeKey() throws Exception {
        return getNodeKey(this.mask, 1);
    }

    public byte[] getMinNodeKey() throws Exception {
        return getNodeKey(this.mask, MinCurrent);
    }

    public byte[] getNextNodeKey() throws Exception {
        this.current++;
        byte[] nodeKey = getNodeKey(this.mask, this.current);
        this.saveSelf();
        return nodeKey;
    }

    private static byte[] getNodeKey(byte[] mask, int number) throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        StreamingUtils.writeTLBytes(mask, stream);
        StreamingUtils.writeIntBigending(number, stream);
        return stream.toByteArray();
    }









    private void saveSelf() throws Exception {

        byte[] selfNodeKey = getSelfNodeKey(this.mask);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();

        StreamingUtils.writeInt(this.current, stream);

        byte[] selfNodeValue = stream.toByteArray();

        this.rocksDB.put(selfNodeKey, selfNodeValue);
    }


}
