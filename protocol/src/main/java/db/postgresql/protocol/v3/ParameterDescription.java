package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;

public class ParameterDescription extends Response {

    private final int[] oids;
    
    public int[] getOids() {
        return oids;
    }

    private ParameterDescription(final Stream stream) {
        super(BackEnd.ParameterDescription);
        final int num = stream.getShort() & 0xFFFF;
        this.oids = new int[num];
        for(int i = 0; i < num; ++i) {
            this.oids[i] = stream.getInt();
        }
    }

    public static final ResponseBuilder builder = new ResponseBuilder() {
            public ParameterDescription build(final BackEnd backEnd, final int size, final Stream stream) {
                return new ParameterDescription(stream);
            }
        };
}
