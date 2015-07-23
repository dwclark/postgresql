package db.postgresql.protocol.v3;

public interface ResponseBuilder {
    Response build(BackEnd backEnd, int size, PostgresqlStream stream);
}
