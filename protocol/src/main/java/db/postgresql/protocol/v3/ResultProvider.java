package db.postgresql.protocol.v3;

public interface ResultProvider {
    void advance();
    Response getResponse();
}
