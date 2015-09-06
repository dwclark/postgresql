package db.postgresql.protocol.v3;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public abstract class Authentication extends Response {

    private Authentication(BackEnd backEnd) {
        super(backEnd);
    }

    public static final ResponseBuilder builder = new ResponseBuilder() {
            public Authentication build(final BackEnd ignore, final int size, final Session session) {
                BackEnd backEnd = BackEnd.find(ignore.id, (byte) session.getInt());
                switch(backEnd) {
                case AuthenticationOk:
                    return Ok.instance;
                case AuthenticationCleartextPassword:
                    return Password.instance;
                case AuthenticationMD5Password:
                    return new Md5(session);
                default:
                    return Fail.instance;
                }
            }
        };

    public static class Fail extends Authentication {
        public static final Fail instance = new Fail();
        private Fail() {
            super((BackEnd) null);
        }
    }

    public static class Ok extends Authentication {
        public static final Ok instance = new Ok();
        private Ok() {
            super(BackEnd.AuthenticationOk);
        }
    }

    public static class Password extends Authentication {
        public static final Password instance = new Password();
        private Password() {
            super(BackEnd.AuthenticationCleartextPassword);
        }
    }

    public static class Md5 extends Authentication {

        final private byte[] salt;

        public byte[] getSalt() {
            return salt;
        }
        
        private Md5(final PostgresqlStream stream) {
            super(BackEnd.AuthenticationMD5Password);
            this.salt = stream.get(new byte[4]);
        }
    }
}
