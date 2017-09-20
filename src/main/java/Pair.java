public class Pair<A, B> {
    private A first;
    private B second;

    A getFirst() {
        return first;
    }

    B getSecond() {
        return second;
    }

    Pair(A a, B b) {
        first = a;
        second = b;
    }
}