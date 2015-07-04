package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.IO;
import db.postgresql.protocol.v3.io.NoData;
import db.postgresql.protocol.v3.io.Stream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class PostgresqlStream extends Stream {
    
    private final static int VERSION = 196608;

    private final Map<BackEnd,ResponseHandler> handlers;

    private static Map<BackEnd,ResponseBuilder> builders() {
        Map<BackEnd, ResponseBuilder> ret = new LinkedHashMap<>();
        ret.put(BackEnd.Authentication, Authentication.builder);
        ret.put(BackEnd.BackendKeyData, KeyData.builder);
        ret.put(BackEnd.BindComplete, Response.builder);
        ret.put(BackEnd.CloseComplete, Response.builder);
        ret.put(BackEnd.CommandComplete, CommandComplete.builder);
        ret.put(BackEnd.CopyData, CopyData.builder);
        ret.put(BackEnd.CopyDone, Response.builder);
        ret.put(BackEnd.DataRow, DataRow.builder);
        ret.put(BackEnd.EmptyQueryResponse, Response.builder);
        ret.put(BackEnd.ErrorResponse, Notice.builder);
        ret.put(BackEnd.NoData, Response.builder);
        ret.put(BackEnd.NoticeResponse, Notice.builder);
        ret.put(BackEnd.NotificationResponse, Notification.builder);
        ret.put(BackEnd.ParameterDescription, ParameterDescription.builder);
        ret.put(BackEnd.ParameterStatus, ParameterStatus.builder);
        ret.put(BackEnd.ParseComplete, Response.builder);
        ret.put(BackEnd.PortalSuspended, Response.builder);
        ret.put(BackEnd.ReadyForQuery, ReadyForQuery.builder);
        ret.put(BackEnd.RowDescription, RowDescription.builder);
        return Collections.unmodifiableMap(ret);
    }

    private static Map<BackEnd,ResponseBuilder> builders = builders();

    public PostgresqlStream(IO io, final Charset encoding,
                            final Map<BackEnd,ResponseHandler> handlers) {
        super(io, encoding);
        //TODO: Add default null handler for all types
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
            list.add(new AbstractMap.SimpleImmutableEntry<>(key, value));
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
        send(true);
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
        send(true);

        return this;
    }

    public PostgresqlStream closeStatement(final String name) {
        return close('S', name);
    }

    public PostgresqlStream closePortal(final String name) {
        return close('P', name);
    }

    public PostgresqlStream bind(final String portal, final String name, final Format[] formats,
                                 final Object[] fields, final Format[] outputFormats) {
        return this;
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

        send(true);
        return this;
    }

    public PostgresqlStream copyDone() {
        put(FrontEnd.CopyDone.toByte());
        putInt(4);
        send(true);
        return this;
    }

    public PostgresqlStream copyFail(final String message) {
        byte[] bytes = message.getBytes(getEncoding());
        put(FrontEnd.CopyFail.toByte());
        putInt(4 + bytes.length + 1); //header + message length + null char
        putNull();
        send(true);
        return this;
    }

    private PostgresqlStream describe(final char type, final String name) {
        byte[] bytes = name.getBytes(getEncoding());
        put(FrontEnd.Describe.toByte());
        putInt(4 + 1 + bytes.length + 1); //size header + type + string + null
        put((byte) type);
        put(bytes);
        putNull();
        send(true);
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
        byte[] bytes = name.getBytes(getEncoding());
        put(FrontEnd.Execute.toByte());
        putInt(4 + bytes.length + 1 + 4); //header + bytes + null + rows
        put(bytes);
        putNull();
        putInt(maxRows);
        send(true);
        return this;
    }

    public PostgresqlStream flush() {
        put(FrontEnd.Flush.toByte());
        putInt(4);
        send(true);
        return this;
    }

    public PostgresqlStream parse(String name, String query, int[] oids) {
        byte[] nameBytes = name.getBytes(getEncoding());
        byte[] queryBytes = query.getBytes(getEncoding());
        put(FrontEnd.Parse.toByte());
        //length of message + portal size + null + query size + null *
        //size of parameter length + oids size
        putInt(4 + nameBytes.length + 1 + queryBytes.length + 1 + 2 + (4 * oids.length));
        put(nameBytes);
        putNull();
        put(queryBytes);
        putNull();
        putShort((short) oids.length);
        for(int oid : oids) {
            putInt(oid);
        }

        send(true);
        return this;
    }

    public PostgresqlStream password(String password) {
        return password(password.getBytes(getEncoding()));
    }

    public PostgresqlStream password(byte[] bytes) {
        put(FrontEnd.Password.toByte());
        putInt(4 + bytes.length);
        put(bytes);
        send(true);
        return this;
    }

    private static String compute(ByteBuffer first, ByteBuffer second) {
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

    public static String md5Hash(Charset c, String user, String password, ByteBuffer salt) {
        ByteBuffer userBytes = ByteBuffer.wrap(user.getBytes(c));
        ByteBuffer passwordBytes = ByteBuffer.wrap(password.getBytes(c));
        return "md5" + compute(ByteBuffer.wrap(compute(passwordBytes, userBytes).getBytes(c)), salt);
    }

    public PostgresqlStream md5(String user, String password, ByteBuffer salt) {
        return password(md5Hash(getEncoding(), user, password, salt));
    }

    public PostgresqlStream query(String str) {
        byte[] bytes = str.getBytes(getEncoding());
        put(FrontEnd.Query.toByte());
        putInt(4 + bytes.length + 1); //header + bytes + null
        put(bytes);
        putNull();
        send(true);
        return this;
    }

    public PostgresqlStream sync() {
        put(FrontEnd.Sync.toByte());
        putInt(4);
        send(true);
        return this;
    }

    public PostgresqlStream terminate() {
        put(FrontEnd.Terminate.toByte());
        putInt(4);
        send(true);
        close();
        return this;
    }

    //back end events
    //next can return null if io timeout happens
    private final AtomicBoolean continueBackground = new AtomicBoolean(true);
    private final ReentrantLock pollingLock = new ReentrantLock();

    public Response next(EnumSet<BackEnd> willHandle) {
        assert(pollingLock.isHeldByCurrentThread());

        BackEnd backEnd;
        try {
            //be sure to not wait forever for response
            backEnd = BackEnd.find(get(1));
        }
        catch(NoData noData) {
            //definitely nothing there
            return null;
        }

        //something is there, we have to finish the action
        Response r = builders.get(backEnd).build(backEnd, getInt() - 4, this);
        if(!willHandle.contains(r.getBackEnd())) {
            handlers.get(r.getBackEnd()).handle(r);
        }

        return r;
    }

    public PostgresqlStream foreground() {
        continueBackground.set(false);
        pollingLock.lock();
        return this;
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
    
    public PostgresqlStream background() {
        pollingLock.unlock();
        continueBackground.set(true);
        new Thread(backgroundRunner).start();
        return this;
    }

    public static Map.Entry<String,String> nullPair(final byte[] bytes, final Charset encoding) {
        int posNull;
        for(posNull = 0; posNull < bytes.length; ++posNull) {
            if(bytes[posNull] == NULL) {
                break;
            }
        }

        return new AbstractMap.SimpleImmutableEntry<>(new String(bytes, 0, posNull, encoding),
                                                      new String(bytes, posNull + 1, bytes.length - (posNull + 2), encoding));

    }
}
