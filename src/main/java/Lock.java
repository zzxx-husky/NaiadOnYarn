public class Lock {
    private boolean value = false;

    void unlock() {
        value = true;
    }

    boolean isLocked() {
        return !value;
    }
}
