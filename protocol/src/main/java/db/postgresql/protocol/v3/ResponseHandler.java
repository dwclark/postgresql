package db.postgresql.protocol.v3;

public interface ResponseHandler {
    void handle(Response r);
}
