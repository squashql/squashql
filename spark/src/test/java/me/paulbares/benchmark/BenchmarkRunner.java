package me.paulbares.benchmark;

public final class BenchmarkRunner {

    public static final BenchmarkRunner INSTANCE = new BenchmarkRunner(false);
    public static final BenchmarkRunner SINGLE = new BenchmarkRunner(true);

    final boolean single;

    private BenchmarkRunner(boolean single) {
        this.single = single;
    }

    /**
     * Runs the performance test and prints the result
     *
     * @param test
     * @throws Exception
     */
    public void run(Benchmark test) throws Exception {
        if (single) {
            final long elapsed = test.run() / 1000_000L;
            System.out.println("Single test time: " + elapsed + "ms");
            return;
        }
        final int numWarms = 2;
        final int numRuns = 10;
        long maxRun = -1;
        long minRun = Long.MAX_VALUE;
        long totalElapsed = 0;

        // Print the header
        final String name = test.getClass().getSimpleName();
        System.out.println(name);
        final StringBuilder sb = new StringBuilder(name.length());
        for (int i = 0; i < name.length(); ++i) {
            sb.append("=");
        }
        System.out.println(sb);

        // Warm up
        System.out.print("Warming up ");
        for (int i = 0; i < numWarms; ++i) {
            System.out.print(".");
            System.out.flush();
            test.run();
        }
        System.out.println(" done!");

        // Run the tests a few times and save the max and min runs
        System.out.print("Running the test " + (numRuns + 2) + " times: ");
        System.out.flush();
        for (int i = 0; i < (numRuns + 2); ++i) {
            final long elapsed = test.run() / 1000_000L;
            if (elapsed > maxRun) {
                maxRun = elapsed;
            }
            if (elapsed < minRun) {
                minRun = elapsed;
            }
            totalElapsed += elapsed;
            System.out.print(elapsed + "ms ");
            System.out.flush();
        }
        System.out.println();

        // Discard the min and max runs and print the average time
        totalElapsed -= maxRun;
        totalElapsed -= minRun;
        final double avgElapsed = ((double) totalElapsed / (double) numRuns);
        System.out.println("Average test time: " + avgElapsed + "ms");
    }
}
