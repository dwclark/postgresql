package db.postgresql.protocol.v3;

public class Extent {
    public Extent() { }
    
    public Extent(final Extent e) {
        this.size = e.size;
        this.position = e.position;
    }
    
    public Extent copy() {
        return new Extent(this);
    }
    
    public int size;
    public int position;

    public int getLast() {
        return (position + size) - 1;
    }

    public int getSize() {
        return size;
    }

    public int getPosition() {
        return position;
    }

    public boolean isNull() {
        return size == -1;
    }
}
