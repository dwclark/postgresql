package db.postgresql.protocol.v3;

public class ParameterDescription extends Response {

    private final int[] oids;
    
    public int[] getOids() {
        return oids;
    }

    private ParameterDescription(final int[] oids) {
        super(BackEnd.ParameterDescription);
        this.oids = oids;
    }

    public static final ResponseBuilder builder = new ResponseBuilder() {
            public ParameterDescription build(final BackEnd backEnd, final int size, final Session session) {
                final int[] oids = new int[session.getShort() & 0xFFFF];
                for(int i = 0; i < oids.length; ++i) {
                    oids[i] = session.getInt();
                }

                return new ParameterDescription(oids);
            }
        };
}
