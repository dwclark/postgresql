package db.postgresql.protocol.v3.ssl;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.cert.X509Certificate;
import java.security.KeyManagementException;
import db.postgresql.protocol.v3.ProtocolException;
import java.security.NoSuchAlgorithmException;
public class ContextCreation {

    private static class NoCert implements X509TrustManager {
        private NoCert() { }
        public static final NoCert instance = new NoCert();
        public static final TrustManager[] trustInstance = new TrustManager[] { instance };
        public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
        public void checkClientTrusted(X509Certificate[] certs, String authType) { }
        public void checkServerTrusted(X509Certificate[] certs, String authType) { }
    }

    public static SSLContext noCert(String algorithm) {
        try {
            SSLContext context = SSLContext.getInstance(algorithm);
            context.init(null, NoCert.trustInstance, null);
            return context;
        }
        catch(KeyManagementException | NoSuchAlgorithmException ex) {
            throw new ProtocolException(ex);
        }
    }

    public static SSLContext noCert() {
        return noCert("TLS");
    }
}
