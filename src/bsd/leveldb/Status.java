package bsd.leveldb;

/**
 * A Status encapsulates the result of an operation.
 * It may indicate success, or it may indicate an error with an associated error message.
*/
public class Status extends RuntimeException {

    public Status() { super(); }
    public Status(String m) { super(m); }
    public Status(Throwable t) { super(t); }
    public Status(String m, Throwable t) { super(m,t); }

    public enum Code {
        OK(0),
        NotFound(1),
        Corruption(2),
        NotSupported(3),
        InvalidArgument(4),
        IOError(5),
        Fault(10);

        public final int k;
        Code(int v) { k=v; }
    }

    public Code state = Code.Fault;

    public Status state(Code state) {
        this.state = state;
        return this;
    }

    @Override
    public String getMessage() {
        String m = super.getMessage();
        String n = state.name();
        return m == null || m.isEmpty() ? n : n+"; "+m;
    }

    public static RuntimeException check(Throwable t) {
        return (t instanceof RuntimeException) ? (RuntimeException)t : new Status(t);
    }

}