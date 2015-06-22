package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;
import db.postgresql.protocol.v3.io.IO;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.AbstractMap;

public class CommandStream extends Stream {
    
    private final Charset encoding;
    private final static int VERSION = 196608;

    public CommandStream(IO io, final Charset encoding) {
        super(io);
        this.encoding = encoding;
    }
    
    //front end requests
    public CommandStream startup(final Map<String,String> keysValues) {
        assert(keysValues != null && !keysValues.isEmpty());
        int size = 9; //size + protocol version size + ending null char
        
        List<Map.Entry<byte[],byte[]>> list = new ArrayList<>(keysValues.size());
        for(Map.Entry<String,String> entry : keysValues.entrySet()) {
            byte[] key = entry.getKey().getBytes(encoding);
            byte[] value = entry.getValue().getBytes(encoding);
            size += (key.length + 1 + value.length + 1);
            list.add(new AbstractMap.SimpleImmutableEntry(key, value));
        }

        //write it to the stream
        putInt(size);
        putInt(VERSION);
        
        for(Map.Entry<byte[], byte[]> entry : list) {
            put(entry.getKey());
            putNull();
            put(entry.getValue());
            putNull();
        }

        sendAll();
        return this;
    }

    private CommandStream close(final char type, final String name) {
        byte[] bytes = name.getBytes(encoding);
        int size = 5 + bytes.length + 1; //header + type + bytes + null terminator

        put(FrontEnd.Close.toByte());
        putInt(size);
        put((byte) type);
        put(bytes);
        putNull();
        sendAll();

        return this;
    }

    public CommandStream closeStatement(final String name) {
        return close('S', name);
    }

    public CommandStream closePortal(final String name) {
        return close('P', name);
    }
    
    public CommandStream copyData(final List<ByteBuffer> buffers) {
        int size = 4; //header
        for(ByteBuffer buffer : buffers) {
            size += buffer.remaining();
        }

        putInt(size);

        for(ByteBuffer buffer : buffers) {
            put(buffer);
        }

        sendAll();
        return this;
    }

    public CommandStream copyDone() {
        put(FrontEnd.CopyDone.toByte());
        putInt(4);
        sendAll();
        return this;
    }

    public CommandStream copyFail(final String message) {
        byte[] bytes = message.getBytes(encoding);
        put(FrontEnd.CopyFail.toByte());
        putInt(4 + bytes.length + 1); //header + message length + null char
        putNull();
        sendAll();
        return this;
    }

    private CommandStrean describe(final char type, final String name) {
        bytes[] bytes = name.getBytes(charset);
        put(FrontEnd.Describe.toByte());
        put(4 + 1 + bytes.length + 1); //size header + type + string + null
        put((byte) type);
        put(bytes);
        putNull();
        sendAll();
        return this;
    }

    public CommandStream describeStatement(final String name) {
        return describe('S', name);
    }

    public CommandStream describePortal(final String name) {
        return describe('P', name);
    }

    public CommandStream execute(final String name) {
        return execute(name, 0);
    }

    public CommandStream execute(final String name, final int maxRows) {
        bytes[] bytes = name.getBytes(encoding);
        put(FrontEnd.Execute.toByte());
        putInt(4 + bytes.length + 1 + 4); //header + bytes + null + rows
        put(bytes);
        putNull();
        putInt(maxRows);
        sendAll();
        return this;
    }

    public CommandStream flush() {
        put(FrontEnd.Flush.toByte());
        putInt(4);
        sendAll();
        return this;
    }

    public CommandStream password(String password) {
        return password(password.getBytes(encoding));
    }

    public CommandStream password(byte[] bytes) {
        put(FrontEnd.Password.toByte());
        put(4 + bytes.length);
        put(bytes);
        sendAll();
        return this;
    }

    public CommandStream query(String str) {
        byte[] bytes = str.getBytes(encoding);
        put(FrontEnd.Query.toByte());
        put(4 + bytes.length + 1); //header + bytes + null
        putNull();
        sendAll();
        return this;
    }

    public CommandStream ssl() {
        putInt(8);
        putInt(FrontEnd.SSLRequest.code);
        sendAll();
        return this;
    }

    public CommandStream sync() {
        put(FrontEnd.Sync.toByte());
        putInt(4);
        sendAll();
        return this;
    }

    public CommandStream terminate() {
        put(FrontEnd.Terminate.toByte());
        putInt(4);
        sendAll();
        return this;
    }

    //back end events
}
