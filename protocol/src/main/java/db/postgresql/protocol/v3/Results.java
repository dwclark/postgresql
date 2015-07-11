package db.postgresql.protocol.v3;

import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class Results {
    public abstract ResultType getResultType();
    public abstract CommandComplete getCommandComplete();
    public abstract Iterator<DataRow> rows();
    public abstract boolean isConsumed();

    public static Results nextResults(final ResultProvider provider) {
        Response response = provider.getResponse();
        switch(response.getBackEnd()) {
        case EmptyQueryResponse: return new EmptyResults();
        case CommandComplete: return new CommandCompleteResults((CommandComplete) response);
        case RowDescription: return new DataRowResults(provider);
        default: return null;
        }
    }
    
    private static class EmptyResults extends Results {
        public ResultType getResultType() { return ResultType.EMPTY; }
        public CommandComplete getCommandComplete() { return null; }
        public Iterator<DataRow> rows() { return null; }
        public boolean isConsumed() { return true; }
    }

    private static class CommandCompleteResults extends Results {
        private final CommandComplete commandComplete;
        
        public CommandCompleteResults(final CommandComplete val) {
            this.commandComplete = val;
        }
        
        public ResultType getResultType() { return ResultType.NO_RESULTS; }
        public CommandComplete getCommandComplete() { return commandComplete; }
        public Iterator<DataRow> rows() { return null; }
        public boolean isConsumed() { return true; }
    }

    private static class DataRowResults extends Results {
        protected final ResultProvider provider;
        protected final RowDescription rowDescription;
        protected CommandComplete commandComplete;
        
        public DataRowResults(final ResultProvider provider) {
            this.provider = provider;
            this.rowDescription = (RowDescription) provider.getResponse();
        }
        
        public ResultType getResultType() { return ResultType.HAS_RESULTS; }
        public CommandComplete getCommandComplete() { return commandComplete; };
        public boolean isConsumed() { return commandComplete != null; }

        public Iterator<DataRow> rows() { return new DataRowIterator(); }

        private class DataRowIterator implements Iterator<DataRow> {
            private DataRow _next;
            private Boolean _hasNext;
            
            private void advance() {
                provider.advance();
                final Response response = provider.getResponse();
                if(response.getBackEnd() == BackEnd.DataRow) {
                    _next = (DataRow) response;
                    _next.setRowDescription(rowDescription);
                    _hasNext = Boolean.TRUE;
                }
                else if(response.getBackEnd() == BackEnd.CommandComplete) {
                    commandComplete = (CommandComplete) response;
                    _next = null;
                    _hasNext = Boolean.FALSE;
                }
            }
            
            public boolean hasNext() {
                if(_hasNext == Boolean.FALSE) {
                    return false;
                }
                else if(_hasNext == Boolean.TRUE){
                    return true;
                }
                
                advance();
                return _hasNext;
            }
            
            public DataRow next() {
                if(_next != null) {
                    _hasNext = null;
                    return _next;
                }
                else {
                    throw new NoSuchElementException();
                }
            }
            
            public void remove() {
                throw new UnsupportedOperationException();
            }
        }
    }
}
