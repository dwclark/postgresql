package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;
import db.postgresql.protocol.v3.io.IO;
import db.postgresql.protocol.v3.io.NoData;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.AbstractMap;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.math.BigInteger;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class PostgresqlStream extends Stream {
    
    private final static int VERSION = 196608;

    private final Map<BackEnd,ResponseBuilder> builders;
    private final Map<BackEnd,ResponseHandler> handlers;

    public PostgresqlStream(IO io, final Charset encoding,
                            final Map<BackEnd,ResponseBuilder> builders,
                            final Map<BackEnd,ResponseHandler> handlers) {
        super(io, encoding);
        this.builders = Collections.unmodifiableMap(builders);
        this.handlers = Collections.unmodifiableMap(handlers);
    }
    
    //front end requests
    public PostgresqlStream startup(final Map<String,String> keysValues) {
        assert(keysValues != null && !keysValues.isEmpty());
        int size = 9; //size + protocol version size + ending null char
        
        List<Map.Entry<byte[],byte[]>> list = new ArrayList<>(keysValues.size());
        for(Map.Entry<String,String> entry : keysValues.entrySet()) {
            byte[] key = entry.getKey().getBytes(getEncoding());
            byte[] value = entry.getValue().getBytes(getEncoding());
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

        putNull();
        sendAll();
        return this;
    }

    private PostgresqlStream close(final char type, final String name) {
        byte[] bytes = name.getBytes(getEncoding());
        int size = 5 + bytes.length + 1; //header + type + bytes + null terminator

        put(FrontEnd.Close.toByte());
        putInt(size);
        put((byte) type);
        put(bytes);
        putNull();
        sendAll();

        return this;
    }

    public PostgresqlStream closeStatement(final String name) {
        return close('S', name);
    }

    public PostgresqlStream closePortal(final String name) {
        return close('P', name);
    }
    
    public PostgresqlStream copyData(final List<ByteBuffer> buffers) {
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

    public PostgresqlStream copyDone() {
        put(FrontEnd.CopyDone.toByte());
        putInt(4);
        sendAll();
        return this;
    }

    public PostgresqlStream copyFail(final String message) {
        byte[] bytes = message.getBytes(getEncoding());
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

    public PostgresqlStream describeStatement(final String name) {
        return describe('S', name);
    }

    public PostgresqlStream describePortal(final String name) {
        return describe('P', name);
    }

    public PostgresqlStream execute(final String name) {
        return execute(name, 0);
    }

    public PostgresqlStream execute(final String name, final int maxRows) {
        bytes[] bytes = name.getBytes(getEncoding());
        put(FrontEnd.Execute.toByte());
        putInt(4 + bytes.length + 1 + 4); //header + bytes + null + rows
        put(bytes);
        putNull();
        putInt(maxRows);
        sendAll();
        return this;
    }

    public PostgresqlStream flush() {
        put(FrontEnd.Flush.toByte());
        putInt(4);
        sendAll();
        return this;
    }

    public PostgresqlStream password(String password) {
        return password(password.getBytes(getEncoding()));
    }

    public PostgresqlStream password(byte[] bytes) {
        put(FrontEnd.Password.toByte());
        put(4 + bytes.length);
        put(bytes);
        sendAll();
        return this;
    }

    private static String compute(byte[] first, byte[] second) {
        try {
            MessageDigest m = MessageDigest.getInstance("MD5");
            m.update(first);
            m.update(second);
            return new BigInteger(1, m.digest()).toString(16);
        }
        catch(NoSuchAlgorithmException e) {
            throw new ProtocolException(e);
        }
    }

    public PostgresqlStream md5(String user, String password, byte[] salt) {
         byte[] userBytes = user.getBytes(c);
         byte[] passwordBytes = password.getBytes(c);
         return password("md5" + compute(compute(passwordBytes, userBytes).getBytes(getEncoding()), salt));
    }

    public PostgresqlStream query(String str) {
        byte[] bytes = str.getBytes(getEncoding());
        put(FrontEnd.Query.toByte());
        put(4 + bytes.length + 1); //header + bytes + null
        putNull();
        sendAll();
        return this;
    }

    public PostgresqlStream ssl() {
        putInt(8);
        putInt(FrontEnd.SSLRequest.code);
        sendAll();
        return this;
    }

    public PostgresqlStream sync() {
        put(FrontEnd.Sync.toByte());
        putInt(4);
        sendAll();
        return this;
    }

    public PostgresqlStream terminate() {
        put(FrontEnd.Terminate.toByte());
        putInt(4);
        sendAll();
        return this;
    }

    //back end events
    //next can return null if io timeout happens
    private final AtomicBoolean continueBackground = new AtomicBoolean(true);
    private final ReentrantLock pollingLock = new ReentrantLock();

    public Response next(EnumSet<BackEnd> willHandle) {
        assert(lock.isHeldByCurrentThread());
        
        try {
            //be sure to not wait forever for response
            BackEnd backEnd = BackEnd.find(get(1));
        }
        catch(NoData noData) {
            //definitely nothing there
            return null;
        }

        //something is there, we have to finish the action
        return builders.get(backEnd).build(backEnd, getInt() - 4, this);
    }

    public void foreground() {
        continueBackground.set(false);
        pollingLock.lock();
    }

    private final Runnable backgroundRunner = new Runnable() {
            public void run() {
                try {
                    pollingLock.lock();
                    EnumSet<BackEnd> none = EnumSet.noneOf(BackEnd.class);
                    while(continueBackground.get()) {
                        Response r = next(none);
                        if(r != null) {
                            handlers.get(r.getBackEnd()).handle(r);
                        }
                    }
                }
                finally {
                    pollingLock.unlock();
                }
            }
        };
    
    public void background() {
        pollingLock.unlock();
        continueBackground.set(true);
        Thread.start(backgroundRunner);
    }
}
