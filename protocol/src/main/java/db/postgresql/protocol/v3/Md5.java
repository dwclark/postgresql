package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.PostgresqlStream;

public class Md5 extends Response {
    
    final private byte[] salt;
    
    public byte[] getSalt() {
        return salt;
    }
    
    public Md5(final PostgresqlStream stream, final int size) {
        super(BackEnd.AuthenticationMD5Password, 4);
        this.salt = stream.get(new byte[4]);
    }
}
