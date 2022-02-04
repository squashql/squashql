package me.paulbares.benchmark;

public interface Benchmark {

    void compute() throws Exception;

    default long run() throws Exception {
        for (int i = 0; i < 4; i++) {
            Runtime.getRuntime().runFinalization();
            Runtime.getRuntime().gc();
        }

        long start = System.nanoTime();
        compute();
        return System.nanoTime() - start;
    }
}