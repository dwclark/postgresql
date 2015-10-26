package db.postgresql.protocol.v3.io;

import db.postgresql.protocol.v3.Bindable;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.FrontEnd;
import db.postgresql.protocol.v3.ProtocolException;
import db.postgresql.protocol.v3.serializers.*;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PostgresqlStream extends NetworkStream {
    
    private final static int VERSION = 196608;
    
    public PostgresqlStream(IO io, final Charset encoding) {
        super(io, encoding);
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
        return this;
    }

    public PostgresqlStream closeStatement(final String name) {
        return close('S', name);
    }

    public PostgresqlStream closePortal(final String name) {
        return close('P', name);
    }

    public PostgresqlStream bind(final String portal, final String name,
                                 final Bindable[] inputs, final Format[] outputs) {

        //lengths
        int total = 4 + portal.length() + 1 + name.length() + 1; //header + lengths + null terminators;
        total += 2 + (2 * inputs.length); //format code header + format code size
        int[] sizes = new int[inputs.length];
        total += 2; //header/size actual parameters
        for(int i = 0; i < inputs.length; ++i) {
            sizes[i] = inputs[i].getLength();
            total += 4;
            if(sizes[i] > 0) {
                total += sizes[i];
            }
        }

        total += 2 + (2 * outputs.length);

        //format stream;
        put(FrontEnd.Bind.toByte());
        putInt(total);
        putCharSequence(portal, Serializer.ASCII_ENCODING);
        putNull();
        putCharSequence(name, Serializer.ASCII_ENCODING);
        putNull();

        //formats
        putShort((short) inputs.length);
        for(Bindable b : inputs) {
            putShort((short) b.getFormat().id);
        }

        //parameters
        putShort((short) inputs.length);
        for(int i = 0; i < inputs.length; ++i) {
            putInt(sizes[i]);
            inputs[i].write(this);
        }

        //results
        putShort((short) outputs.length);
        for(Format f : outputs) {
            putShort((short) f.id);
        }

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

        return this;
    }

    public PostgresqlStream copyDone() {
        put(FrontEnd.CopyDone.toByte());
        putInt(4);
        return this;
    }

    public PostgresqlStream copyFail(final String message) {
        byte[] bytes = message.getBytes(getEncoding());
        put(FrontEnd.CopyFail.toByte());
        putInt(4 + bytes.length + 1); //header + message length + null char
        putNull();
        return this;
    }

    private PostgresqlStream describe(final char type, final String name) {
        byte[] bytes = name.getBytes(getEncoding());
        put(FrontEnd.Describe.toByte());
        putInt(4 + 1 + bytes.length + 1); //size header + type + string + null
        put((byte) type);
        put(bytes);
        putNull();
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
        return this;
    }

    public PostgresqlStream flush() {
        put(FrontEnd.Flush.toByte());
        putInt(4);
        return this;
    }

    public static final int[] EMPTY_OIDS = new int[0];
    public static final Format[] EMPTY_FORMATS = new Format[0];
    
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

        return this;
    }

    public PostgresqlStream password(String password) {
        return password(password.getBytes(getEncoding()));
    }

    public PostgresqlStream password(byte[] bytes) {
        put(FrontEnd.Password.toByte());
        putInt(4 + bytes.length);
        put(bytes);
        return this;
    }

    private static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for(byte b : bytes) {
            sb.append(String.format("%02x", b));
        }

        return sb.toString();
    }
    
    private static String compute(ByteBuffer first, ByteBuffer second) {
        try {
            MessageDigest m = MessageDigest.getInstance("MD5");
            m.update(first);
            m.update(second);
            return toHex(m.digest());
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
        return this;
    }

    public PostgresqlStream sync() {
        put(FrontEnd.Sync.toByte());
        putInt(4);
        return this;
    }

    public PostgresqlStream terminate() {
        put(FrontEnd.Terminate.toByte());
        putInt(4);
        
        close();
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
