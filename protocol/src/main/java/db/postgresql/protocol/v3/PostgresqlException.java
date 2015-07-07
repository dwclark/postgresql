package db.postgresql.protocol.v3;

import java.util.Map;

public class PostgresqlException extends RuntimeException {

    private final Map<NoticeType,String> contents;
    
    public PostgresqlException(final Map<NoticeType,String> contents) {
        this.contents = contents;
    }

    public String getLine() {
        return contents.get(NoticeType.Line);
    }

    public String getCode() {
        return contents.get(NoticeType.Code);
    }

    public String getSeverity() {
        return contents.get(NoticeType.Severity);
    }

    @Override
    public String getMessage() {
        return contents.get(NoticeType.Message);
    }

    public String getRoutine() {
        return contents.get(NoticeType.Routine);
    }

    public String getFile() {
        return contents.get(NoticeType.File);
    }
}
