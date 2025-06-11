package jdev.kovalev;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws InterruptedException {
        try {
            MapReduce.clearTmpAndOutputDirectories();
        } catch (IOException e) {
            logger.error("Ошибка при очистке директорий");
            throw new RuntimeException(e);
        }

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