package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Arrays;
import java.nio.charset.Charset;

public class RowDescription extends Response {

    private List<FieldDescriptor> fields;
    private int num;

    public List<FieldDescriptor> getFields() {
        if(fields == null) {
            FieldDescriptor[] descriptors = new FieldDescriptor[num];
            for(int i = 0; i < num; ++i) {
                descriptors[i] = FieldDescriptor.from(this);
            }

            buffer.position(0);
            fields = Arrays.asList(descriptors);
        }

        return fields;
    }
    
    private RowDescription() {
        super(BackEnd.RowDescription);
    }

    private RowDescription(RowDescription toCopy) {
        super(toCopy);
        this.num = toCopy.num;
        this.fields = fields;
    }

    @Override
    public RowDescription copy() {
        ByteBuffer tmp = buffer;
        buffer = null;
        RowDescription ret = new RowDescription(this);
        buffer = tmp;
        return ret;
    }

    public RowDescription reset(ByteBuffer buffer, Charset encoding, int num) {
        super.reset(buffer, encoding);
        this.num = num;
        this.fields = null;
        return this;
    }

    private static final ThreadLocal<RowDescription> tlData = new ThreadLocal<RowDescription>() {
            @Override protected RowDescription initialValue() {
                return new RowDescription();
            }
        };
    
    public static final ResponseBuilder builder = new ResponseBuilder() {
            public RowDescription build(final BackEnd backEnd, final int size, final Stream stream) {
                int num = 0xFFFF & stream.getShort();
                return (RowDescription) tlData.get().reset(stream.getRecord(size-2), stream.getEncoding(), num);
            }
        };
}
