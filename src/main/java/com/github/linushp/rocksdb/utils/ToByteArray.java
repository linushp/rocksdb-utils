package com.github.linushp.rocksdb.utils;

public interface ToByteArray {
    byte[] toByteArray();
    Object fromByteArray(byte[] bytes);
}
