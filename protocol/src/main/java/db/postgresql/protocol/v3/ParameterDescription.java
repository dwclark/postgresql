package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;

public class ParameterDescription extends Response {

    public int[] getOids() {
        int[] ret = new int[buffer.remaining() / 4];
        buffer.asIntBuffer().get(ret);
        buffer.position(0);
        return ret;
    }

    private ParameterDescription() {
        super(BackEnd.ParameterDescription);
    }

    private ParameterDescription(ParameterDescription toCopy) {
        super(toCopy);
    }

    @Override
    public ParameterDescription copy() {
        return new ParameterDescription(this);
    }

    private static final ThreadLocal<ParameterDescription> tlData = new ThreadLocal<ParameterDescription>() {
            @Override protected ParameterDescription initialValue() {
                return new ParameterDescription();
            }
        };

    public static final ResponseBuilder builder = new ResponseBuilder() {
            public ParameterDescription build(final BackEnd backEnd, final int size, final Stream stream) {
                final int num = stream.getShort() & 0xFFFF;
                return (ParameterDescription) tlData.get().reset(stream.getRecord(num * 4), stream.getEncoding());
            }
        };
}
