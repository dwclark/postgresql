package db.postgresql.protocol.v3;

public class KeyData extends Response {

    private final int pid;
    private final int secretKey;
    
    public int getPid() {
        return pid;
    }

    public int getSecretKey() {
        return secretKey;
    }
    
    private KeyData(final int pid, final int secretKey) {
        super(BackEnd.BackendKeyData);
        this.pid = pid;
        this.secretKey = secretKey;
    }

    public static final ResponseBuilder builder = new ResponseBuilder() {
            public KeyData build(final BackEnd backEnd, final int size, final Session session) {
                return new KeyData(session.getInt(), session.getInt());
            }
        };
}
