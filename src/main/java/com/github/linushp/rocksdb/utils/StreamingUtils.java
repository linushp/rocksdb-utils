package com.github.linushp.rocksdb.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;


public class StreamingUtils {


    public static void writeByte(int v, OutputStream stream) throws IOException {
        stream.write(v);
    }

    public static void writeByte(Byte v, OutputStream stream) throws IOException {
        if (v == null) {
            v = 0;
        }
        stream.write(v);
    }

    public static byte readByte(InputStream stream) throws IOException {
        int a = stream.read();
        if (a < 0) {
            throw new IOException();
        }
        return (byte) a;
    }


    public static void writeInt(Integer v, OutputStream stream) throws IOException {
        if (v == null) {
            v = 0;
        }
        writeByte((byte) (v & 0xFF), stream);
        writeByte((byte) ((v >> 8) & 0xFF), stream);
        writeByte((byte) ((v >> 16) & 0xFF), stream);
        writeByte((byte) ((v >> 24) & 0xFF), stream);
    }

    public static void writeInt(int v, byte[] buffer, int offset) {
        buffer[offset] = (byte) (v & 0xFF);
        buffer[offset + 1] = (byte) ((v >> 8) & 0xFF);
        buffer[offset + 2] = (byte) ((v >> 16) & 0xFF);
        buffer[offset + 3] = (byte) ((v >> 24) & 0xFF);
    }

    public static void writeIntBigending(int v, OutputStream stream) throws IOException {
        writeByte((byte) ((v >> 24) & 0xFF), stream);
        writeByte((byte) ((v >> 16) & 0xFF), stream);
        writeByte((byte) ((v >> 8) & 0xFF), stream);
        writeByte((byte) (v & 0xFF), stream);
    }

    public static void writeIntBigending(int v, byte[] buffer, int offset) {
        buffer[offset] = (byte) ((v >> 24) & 0xFF);
        buffer[offset + 1] = (byte) ((v >> 16) & 0xFF);
        buffer[offset + 2] = (byte) ((v >> 8) & 0xFF);
        buffer[offset + 3] = (byte) (v & 0xFF);
    }


    public static void writeLong(Long v, OutputStream stream) throws IOException {
        if (v == null) {
            v = 0L;
        }
        writeByte((byte) (v & 0xFF), stream);
        writeByte((byte) ((v >> 8) & 0xFF), stream);
        writeByte((byte) ((v >> 16) & 0xFF), stream);
        writeByte((byte) ((v >> 24) & 0xFF), stream);

        writeByte((byte) ((v >> 32) & 0xFF), stream);
        writeByte((byte) ((v >> 40) & 0xFF), stream);
        writeByte((byte) ((v >> 48) & 0xFF), stream);
        writeByte((byte) ((v >> 56) & 0xFF), stream);
    }

    public static void writeLong(long v, byte[] buffer, int offset) {
        buffer[offset] = (byte) (v & 0xFF);
        buffer[offset + 1] = (byte) ((v >> 8) & 0xFF);
        buffer[offset + 2] = (byte) ((v >> 16) & 0xFF);
        buffer[offset + 3] = (byte) ((v >> 24) & 0xFF);

        buffer[offset + 4] = (byte) ((v >> 32) & 0xFF);
        buffer[offset + 5] = (byte) ((v >> 40) & 0xFF);
        buffer[offset + 6] = (byte) ((v >> 48) & 0xFF);
        buffer[offset + 7] = (byte) ((v >> 56) & 0xFF);
    }

    public static void writeLongBigending(long v, OutputStream stream) throws IOException {
        writeByte((byte) ((v >> 56) & 0xFF), stream);
        writeByte((byte) ((v >> 48) & 0xFF), stream);
        writeByte((byte) ((v >> 40) & 0xFF), stream);
        writeByte((byte) ((v >> 32) & 0xFF), stream);
        writeByte((byte) ((v >> 24) & 0xFF), stream);
        writeByte((byte) ((v >> 16) & 0xFF), stream);
        writeByte((byte) ((v >> 8) & 0xFF), stream);
        writeByte((byte) (v & 0xFF), stream);
    }

    public static void writeLongBigending(long v, byte[] buffer, int offset) {
        buffer[offset] = (byte) ((v >> 56) & 0xFF);
        buffer[offset + 1] = (byte) ((v >> 48) & 0xFF);
        buffer[offset + 2] = (byte) ((v >> 40) & 0xFF);
        buffer[offset + 3] = (byte) ((v >> 32) & 0xFF);
        buffer[offset + 4] = (byte) ((v >> 24) & 0xFF);
        buffer[offset + 5] = (byte) ((v >> 16) & 0xFF);
        buffer[offset + 6] = (byte) ((v >> 8) & 0xFF);
        buffer[offset + 7] = (byte) (v & 0xFF);

    }


    public static void writeDouble(Double v, OutputStream stream) throws IOException {
        if (v == null) {
            v = 0D;
        }
        writeLong(Double.doubleToLongBits(v), stream);
    }



    private static void writeByteArray(byte[] data, OutputStream stream) throws IOException {
        stream.write(data);
    }


    private static void writeByteArray(byte[] data, int offset, int len, OutputStream stream) throws IOException {
        stream.write(data, offset, len);
    }


    public static void writeTLString(String v, OutputStream stream) throws IOException {
        if (v == null) {
            v = "";
        }
        writeTLBytes(v.getBytes(StandardCharsets.UTF_8), stream);
    }


    public static void writeTLBool(Boolean v, OutputStream stream) throws IOException {
        if (v == null) {
            writeByte(0, stream);
        } else if (v) {
            writeByte(1, stream);
        } else {
            writeByte(0, stream);
        }
    }

    public static boolean readTLBool(InputStream inputStream) throws IOException {
        int x = readByte(inputStream);
        if (x == 1) {
            return true;
        }
        return false;
    }



    public static void writeTLBytes(byte[] v, OutputStream stream) throws IOException {
        if (v == null) {
            v = new byte[]{};
        }

        int startOffset = 1;
        if (v.length >= 254) {
            startOffset = 4;
            writeByte(254, stream);
            writeByte(v.length & 0xFF, stream);
            writeByte((v.length >> 8) & 0xFF, stream);
            writeByte((v.length >> 16) & 0xFF, stream);
        } else {
            writeByte(v.length, stream);
        }

        writeByteArray(v, stream);

        int offset = (v.length + startOffset) % 4;
        if (offset != 0) {
            int offsetCount = 4 - offset;
            writeByteArray(new byte[offsetCount], stream);
        }
    }


    public static void writeTLBytes(TLBytes v, OutputStream stream) throws IOException {
        int startOffset = 1;
        if (v.getLength() >= 254) {
            startOffset = 4;
            writeByte(254, stream);
            writeByte(v.getLength() & 0xFF, stream);
            writeByte((v.getLength() >> 8) & 0xFF, stream);
            writeByte((v.getLength() >> 16) & 0xFF, stream);
        } else {
            writeByte(v.getLength(), stream);
        }

        writeByteArray(v.getData(), v.getOffset(), v.getLength(), stream);

        int offset = (v.getLength() + startOffset) % 4;
        if (offset != 0) {
            int offsetCount = 4 - offset;
            writeByteArray(new byte[offsetCount], stream);
        }
    }



    public static int readInt(InputStream stream) throws IOException {
        int a = stream.read();
        if (a < 0) {
            throw new IOException();
        }
        int b = stream.read();
        if (b < 0) {
            throw new IOException();
        }
        int c = stream.read();
        if (c < 0) {
            throw new IOException();
        }
        int d = stream.read();
        if (d < 0) {
            throw new IOException();
        }

        return a + (b << 8) + (c << 16) + (d << 24);
    }

    public static int readIntBigending(InputStream stream) throws IOException {
        int a = stream.read();
        if (a < 0) {
            throw new IOException();
        }
        int b = stream.read();
        if (b < 0) {
            throw new IOException();
        }
        int c = stream.read();
        if (c < 0) {
            throw new IOException();
        }
        int d = stream.read();
        if (d < 0) {
            throw new IOException();
        }

        return (a << 24) + (b << 16) + (c << 8) + (d);
    }


    public static long readUInt(InputStream stream) throws IOException {
        long a = stream.read();
        if (a < 0) {
            throw new IOException();
        }
        long b = stream.read();
        if (b < 0) {
            throw new IOException();
        }
        long c = stream.read();
        if (c < 0) {
            throw new IOException();
        }
        long d = stream.read();
        if (d < 0) {
            throw new IOException();
        }

        return a + (b << 8) + (c << 16) + (d << 24);
    }

    public static long readUIntBigending(InputStream stream) throws IOException {
        long a = stream.read();
        if (a < 0) {
            throw new IOException();
        }
        long b = stream.read();
        if (b < 0) {
            throw new IOException();
        }
        long c = stream.read();
        if (c < 0) {
            throw new IOException();
        }
        long d = stream.read();
        if (d < 0) {
            throw new IOException();
        }

        return (a << 24) + (b << 16) + (c << 8) + (d);
    }


    public static long readLong(InputStream stream) throws IOException {
        long a = readUInt(stream);
        long b = readUInt(stream);

        return a + (b << 32);
    }

    public static long readLongBigending(InputStream stream) throws IOException {
        long a = readUIntBigending(stream);
        long b = readUIntBigending(stream);

        return (a << 32) + (b);
    }


    public static double readDouble(InputStream stream) throws IOException {
        return Double.longBitsToDouble(readLong(stream));
    }



    public static String readTLString(InputStream stream) throws IOException {
        return new String(readTLBytes(stream), StandardCharsets.UTF_8);
    }



    public static byte[] readBytes(int count, InputStream stream) throws IOException {
        byte[] res = new byte[count];
        int offset = 0;
        while (offset < res.length) {
            int readed = stream.read(res, offset, res.length - offset);
            if (readed > 0) {
                offset += readed;
            } else if (readed < 0) {
                throw new IOException();
            } else {
                Thread.yield();
            }
        }
        return res;
    }


    public static void skipBytes(int count, InputStream stream) throws IOException {
        stream.skip(count);
    }


    public static void readBytes(byte[] buffer, int offset, int count, InputStream stream) throws IOException {
        int woffset = 0;
        while (woffset < count) {
            int readed = stream.read(buffer, woffset + offset, count - woffset);
            if (readed > 0) {
                woffset += readed;
            } else if (readed < 0) {
                throw new IOException();
            } else {
                Thread.yield();
            }
        }
    }


    public static byte[] readTLBytes(InputStream stream) throws IOException {
        int count = stream.read();
        int startOffset = 1;
        if (count >= 254) {
            count = stream.read() + (stream.read() << 8) + (stream.read() << 16);
            startOffset = 4;
        }

        byte[] raw = readBytes(count, stream);
        int offset = (count + startOffset) % 4;
        if (offset != 0) {
            int offsetCount = 4 - offset;
            skipBytes(offsetCount, stream);
        }

        return raw;
    }



    public static byte[] intToBytes(int value) {
        return new byte[]{
                (byte) (value & 0xFF),
                (byte) ((value >> 8) & 0xFF),
                (byte) ((value >> 16) & 0xFF),
                (byte) ((value >> 24) & 0xFF)};
    }

    public static byte[] intToBytesBigending(int value) {
        return new byte[]{
                (byte) ((value >> 24) & 0xFF),
                (byte) ((value >> 16) & 0xFF),
                (byte) ((value >> 8) & 0xFF),
                (byte) (value & 0xFF),
        };
    }


    public static byte[] longToBytes(long value) {
        return new byte[]{
                (byte) (value & 0xFF),
                (byte) ((value >> 8) & 0xFF),
                (byte) ((value >> 16) & 0xFF),
                (byte) ((value >> 24) & 0xFF),
                (byte) ((value >> 32) & 0xFF),
                (byte) ((value >> 40) & 0xFF),
                (byte) ((value >> 48) & 0xFF),
                (byte) ((value >> 56) & 0xFF)};
    }


    public static int readInt(byte[] src) {
        return readInt(src, 0);
    }


    public static int readInt(byte[] src, int offset) {
        int a = src[offset + 0] & 0xFF;
        int b = src[offset + 1] & 0xFF;
        int c = src[offset + 2] & 0xFF;
        int d = src[offset + 3] & 0xFF;

        return a + (b << 8) + (c << 16) + (d << 24);
    }

    public static int readIntBigending(byte[] src, int offset) {
        int a = src[offset + 0] & 0xFF;
        int b = src[offset + 1] & 0xFF;
        int c = src[offset + 2] & 0xFF;
        int d = src[offset + 3] & 0xFF;

        return (a << 24) + (b << 16) + (c << 8) + (d);
    }


    public static long readUInt(byte[] src) {
        return readUInt(src, 0);
    }

    public static long readUInt(byte[] src, int offset) {
        long a = src[offset + 0] & 0xFF;
        long b = src[offset + 1] & 0xFF;
        long c = src[offset + 2] & 0xFF;
        long d = src[offset + 3] & 0xFF;

        return a + (b << 8) + (c << 16) + (d << 24);
    }

    public static long readUIntBigending(byte[] src, int offset) {
        long a = src[offset + 0] & 0xFF;
        long b = src[offset + 1] & 0xFF;
        long c = src[offset + 2] & 0xFF;
        long d = src[offset + 3] & 0xFF;

        return (a << 24) + (b << 16) + (c << 8) + (d);
    }

    public static long readLong(byte[] src, int offset) {
        long a = readUInt(src, offset);
        long b = readUInt(src, offset + 4);
        return (a & 0xFFFFFFFF) + ((b & 0xFFFFFFFF) << 32);
    }

    public static long readLongBigending(byte[] src, int offset) {
        long a = readUInt(src, offset);
        long b = readUInt(src, offset + 4);
        return ((a & 0xFFFFFFFF) << 32) + (b & 0xFFFFFFFF);
    }
}
