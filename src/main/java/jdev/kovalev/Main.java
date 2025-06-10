package jdev.kovalev;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws InterruptedException {

        List<String> inputFiles = List.of("1.txt", "2.txt");

        int nReduce = 3;
        int nWorkers = 4;

        Coordinator coordinator = new Coordinator(inputFiles, nReduce);
        ExecutorService executor = Executors.newFixedThreadPool(nWorkers);

        for (int i = 0; i < nWorkers; i++) {
            executor.execute(new Worker(coordinator, nReduce));
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);
    }
}