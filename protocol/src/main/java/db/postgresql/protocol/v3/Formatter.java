package db.postgresql.protocol.v3;

import java.nio.charset.Charset;

public class Formatter {
    public static final Charset charset = Charset.forName("UTF-8");
    public static final byte NULL = (byte) 0;
}
