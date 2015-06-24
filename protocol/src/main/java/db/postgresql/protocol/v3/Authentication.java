package db.postgresql.protocol.v3;

import db.postgresql.protocol.v3.io.Stream;
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
            public Authentication build(final BackEnd ignore, final int size, final Stream stream) {
                BackEnd backEnd = BackEnd.find(ignore.id, (byte) stream.getInt());
                switch(backEnd) {
                case AuthenticationOk: return new Ok();
                case AuthenticationCleartextPassword: return new Password();
                case AuthenticationMD5Password:
                    return new Md5(stream.get(new byte[4]));
                default: return new Fail();
                }
            }
        };

    public static class Fail extends Authentication {
        public Fail() {
            super(null);
        }
    }

    public static class Ok extends Authentication {
        public Ok() {
            super(BackEnd.AuthenticationOk);
        }
    }

    public static class Password extends Authentication {
        public Password() {
            super(BackEnd.AuthenticationCleartextPassword);
        }
    }

    public static class Md5 extends Authentication {
        private final byte[] salt;
        
        public Md5(final byte[] salt) {
            super(BackEnd.AuthenticationMD5Password);
            this.salt = salt;
        }

        public byte[] getSalt() {
            return salt;
        }
    }
}
