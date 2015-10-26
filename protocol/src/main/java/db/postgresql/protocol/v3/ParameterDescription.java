package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.PostgresqlStream;

public class ParameterDescription extends Response {

    private final int[] oids;
    
    public int[] getOids() {
        return oids;
    }

    public ParameterDescription(final PostgresqlStream stream, final int size) {
        super(BackEnd.ParameterDescription, size);
        this.oids = new int[stream.getShort() & 0xFFFF];
        for(int i = 0; i < oids.length; ++i) {
            oids[i] = stream.getInt();
        }
    }
}
