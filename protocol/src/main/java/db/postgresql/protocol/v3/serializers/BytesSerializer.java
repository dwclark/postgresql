package db.postgresql.protocol.v3.serializers;

import db.postgresql.protocol.v3.Bindable;
import db.postgresql.protocol.v3.Format;
import db.postgresql.protocol.v3.io.Stream;
import db.postgresql.protocol.v3.io.Stream;
import java.nio.ByteBuffer;
import db.postgresql.protocol.v3.typeinfo.PgType;

public class BytesSerializer extends Serializer<byte[]> {

    public static final PgType PGTYPE =
        new PgType.Builder().name("bytea").oid(17).arrayId(1001).build();

    public static final BytesSerializer instance = new BytesSerializer();
    
    private BytesSerializer() {
        super(byte[].class);
    }
        
    public byte[] read(final Stream stream, final int size, final Format format) {
        if(size == NULL_LENGTH) {
            return null;
        }

        stream.get(); stream.get(); //advance past escape sequence
        byte[] ret = new byte[(size - 2) / 2];
        for(int i = 0; i < ret.length; ++i) {
            ret[i] = (byte) ((Character.digit(stream.get(), 16) << 4) + Character.digit(stream.get(), 16));
        }

        return ret;
    }

    public int length(final byte[] bytes, final Format format) {
        return (bytes == null) ? NULL_LENGTH : (2 + (bytes.length * 2));
    }

    final private static byte[] asciiHexArray = { (byte) '0', (byte) '1', (byte) '2', (byte) '3', (byte) '4',
                                                  (byte) '5', (byte) '6', (byte) '7', (byte) '8', (byte) '9',
                                                  (byte) 'a', (byte) 'b', (byte) 'c', (byte) 'd', (byte) 'e', (byte) 'f' };
    
    public void write(final Stream stream, final byte[] bytes, final Format format) {
        stream.put((byte) '\\').put((byte) 'x');
        for(int i = 0; i < bytes.length; ++i) {
            stream.put(asciiHexArray[bytes[i] >>> 4]);
            stream.put(asciiHexArray[bytes[i] & 0x0F]);
        }
    }
}
