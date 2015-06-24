package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;

public class ParameterDescription extends Response {

    private final int[] oids;

    public int[] getOids() {
        return oids;
    }

    public ParameterDescription(final BackEnd backEnd, final int[] oids) {
        super(backEnd);
        this.oids = oids;
    }

    public static final ResponseBuilder builder = new ResponseBuilder() {
            public ParameterDescription build(final BackEnd backEnd, final int size, final Stream stream) {
                int[] oids = new int[stream.getShort() & 0xFFFF];
                for(int i = 0; i < oids.length; ++i) {
                    oids[i] = stream.getInt();
                }

                return new ParameterDescription(backEnd, oids);
            }
        };
}
