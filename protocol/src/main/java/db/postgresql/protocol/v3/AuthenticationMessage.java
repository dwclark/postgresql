package db.postgresql.protocol.v3;

import java.nio.ByteBuffer;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.math.BigInteger;
import java.nio.charset.Charset;

public abstract class AuthenticationMessage extends BackEndMessage {

    private AuthenticationMessage(BackEnd backEnd) {
        super(backEnd);
    }

    public abstract boolean negotiate(Session session);

    public static final BackEndBuilder builder = new BackEndBuilder() {
            public AuthenticationMessage read(final BackEnd ignore, final int size, final Session session) {
                ByteBuffer buffer = session.read(size);
                BackEnd backEnd = BackEnd.find(ignore.id, (byte) buffer.getInt());
                switch(backEnd) {
                case AuthenticationOk: return new Ok();
                case AuthenticationCleartextPassword: return new Password();
                case AuthenticationMD5Password:
                    byte[] salt = new byte[4];
                    buffer.get(salt);
                    return new Md5(salt);
                default: return new Fail();
                }
            }
        };

    private static class Fail extends AuthenticationMessage {
        public Fail() {
            super(null);
        }

        public boolean negotiate(Session session) {
            return false;
        }
    }

    private static class Ok extends AuthenticationMessage {
        public Ok() {
            super(BackEnd.AuthenticationOk);
        }

        public boolean negotiate(Session session) {
            return true;
        }
    }

    private static class Password extends AuthenticationMessage {
        public Password() {
            super(BackEnd.AuthenticationCleartextPassword);
        }

        public boolean negotiate(Session session) {
            Formatter f = session.getFormatter();
            session.write(f.password(session.getPassword()));
            BackEndMessage response = session.next();
            return response.getBackEnd() == BackEnd.AuthenticationOk;
        }
    }

    private static class Md5 extends AuthenticationMessage {
        private byte[] salt;
        
        public Md5(byte[] salt) {
            super(BackEnd.AuthenticationMD5Password);
            this.salt = salt;
        }

        private static String compute(byte[] first, byte[] second) {
            try {
                MessageDigest m = MessageDigest.getInstance("MD5");
                m.update(first);
                m.update(second);
                return new BigInteger(1, m.digest()).toString(16);
            }
            catch(NoSuchAlgorithmException e) {
                throw new ProtocolException(e);
            }
        }

        public static String payload(Charset c, String strUser, String strPassword, byte[] salt) {
            byte[] user = strUser.getBytes(c);
            byte[] password = strPassword.getBytes(c);
            return "md5" + compute(compute(password, user).getBytes(c), salt);
        }
        
        public boolean negotiate(Session session) {
            Formatter f = session.getFormatter();
            session.write(f.password(payload(f.getCharset(), session.getUser(),
                                             session.getPassword(), salt)));
            BackEndMessage response = session.next();
            return response.getBackEnd() == BackEnd.AuthenticationOk;
        }
    }
}
