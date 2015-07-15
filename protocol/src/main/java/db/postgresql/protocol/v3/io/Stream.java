package db.postgresql.protocol.v3.io;

import db.postgresql.protocol.v3.ProtocolException;
import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CharsetDecoder;

public interface Stream {

    public static final byte NULL = (byte) 0;

    void close();
    Charset getEncoding();
    void send(boolean sendAll);
    void recv();
    void recv(int atLeast);
    void recv(int atLeast, int tries);
    void advance(int size);
    Stream put(ByteBuffer buffer);
    Stream put(byte b);
    Stream put(byte[] bytes);
    Stream putShort(short s);
    Stream putInt(int i);
    Stream putNull();
    Stream putCharSequence(CharSequence seq);
    Stream putCharSequence(CharSequence seq, Charset encoding);
    Stream putString(String str);
    ByteBuffer view(int max);
    ByteBuffer getBuffer(ByteBuffer buffer);
    byte get();
    byte get(int tries);
    byte[] get(byte[] dst, int offset, int length);
    byte[] get(byte[] dst, int offset, int length, int tries);
    byte[] get(byte[] dst);
    byte[] get(byte[] dst, int tries);
    String nullString();
    String nullString(int size);
    String nullString(int size, int tries);
    short getShort();
    short getShort(int tries);
    int getInt();
    int getInt(final int tries);
    byte getNull();
    byte getNull(int tries);
    CharBuffer getCharBuffer(int numBytes);
}
