package db.postgresql.protocol.v3.io;

import db.postgresql.protocol.v3.ProtocolException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CharacterCodingException;
import java.util.List;
import java.util.ArrayList;

public class BindingStream implements Stream {

    private static final int MULTIPLIER = 32;

    private final Charset encoding;
    private final List<Field> fields;
    private ByteBuffer buffer;
    
    public BindingStream(final int numberParameters, final Charset encoding) {
        this.fields = new ArrayList<>(numberParameters);
        this.buffer = ByteBuffer.allocate(numberParameters * MULTIPLIER);
        this.encoding = encoding;
    }

    public List<Field> getFields() {
        return fields;
    }

    public class Field {
        private int begin;
        private int end;

        public int size() {
            return end - begin;
        }

        public ByteBuffer buffer() {
            buffer.position(begin);
            buffer.limit(end);
            return buffer;
        }
    }

    public void begin() {
        final Field f = new Field();
        f.begin = buffer.position();
        fields.add(f);
    }

    public void end() {
        final Field f = fields.get(fields.size() - 1);
        f.end = buffer.position();
    }
    
    //unimplemented
    public void close() { }
    public Charset getEncoding() { throw new UnsupportedOperationException(); }
    public void send(boolean sendAll) { }
    public void recv() { }
    public void recv(int atLeast) { }
    public void recv(int atLeast, int tries) { }
    public void advance(int size) { }
    public ByteBuffer view(int max) { throw new UnsupportedOperationException(); }
    public ByteBuffer getBuffer(ByteBuffer buffer) { throw new UnsupportedOperationException(); }
    public byte get() { throw new UnsupportedOperationException(); }
    public byte get(int tries) { throw new UnsupportedOperationException(); }
    public byte[] get(byte[] dst, int offset, int length) { throw new UnsupportedOperationException(); }
    public byte[] get(byte[] dst, int offset, int length, int tries) { throw new UnsupportedOperationException(); }
    public byte[] get(byte[] dst) { throw new UnsupportedOperationException(); }
    public byte[] get(byte[] dst, int tries) { throw new UnsupportedOperationException(); }
    public String nullString() { throw new UnsupportedOperationException(); }
    public String nullString(int size) { throw new UnsupportedOperationException(); }
    public String nullString(int size, int tries) { throw new UnsupportedOperationException(); }
    public short getShort() { throw new UnsupportedOperationException(); }
    public short getShort(int tries) { throw new UnsupportedOperationException(); }
    public int getInt() { throw new UnsupportedOperationException(); }
    public int getInt(final int tries) { throw new UnsupportedOperationException(); }
    public byte getNull() { throw new UnsupportedOperationException(); }
    public byte getNull(int tries) { throw new UnsupportedOperationException(); }
    public CharBuffer getCharBuffer(int numBytes) { throw new UnsupportedOperationException(); }

    public Stream put(final ByteBuffer src) {
        ensure(src.remaining());
        buffer.put(src);
        return this;
    }
    
    public Stream put(final byte b) {
        ensure(1);
        buffer.put(b);
        return this;
    }
    
    public Stream put(final byte[] bytes) {
        ensure(bytes.length);
        buffer.put(bytes);
        return this;
    }
    
    public Stream putShort(final short s) {
        ensure(2);
        buffer.putShort(s);
        return this;
    }
    
    public Stream putInt(final int i) {
        ensure(4);
        buffer.putInt(i);
        return this;
    }
    
    public Stream putNull() {
        ensure(1);
        return put(NULL);
    }
    
    public Stream putCharSequence(final CharSequence seq) {
        return putCharSequence(seq, encoding);
    }
    
    public Stream putCharSequence(final CharSequence seq, final Charset encoding) {
        try {
            final CharBuffer charBuffer = CharBuffer.wrap(seq);
            final CharsetEncoder encoder = encoding.newEncoder();
            return put(encoder.encode(charBuffer));
        }
        catch(CharacterCodingException cce) {
            throw new ProtocolException(cce);
        }
    }
    
    public Stream putString(final String str) {
        return putCharSequence(str, encoding);
    }

    private void ensure(final int bytes) {
        if(buffer.remaining() < bytes) {
            final int newCapacity = Math.max(bytes + buffer.capacity(), 2 * buffer.capacity());
            ByteBuffer tmp = ByteBuffer.allocate(newCapacity);
            buffer.flip();
            tmp.put(buffer);
            buffer = tmp;
        }
    }
}
