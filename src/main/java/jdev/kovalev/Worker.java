package jdev.kovalev;

import jdev.kovalev.dto.KeyValue;
import jdev.kovalev.task.Task;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
public class Worker implements Runnable {
    private final Coordinator coordinator;
    private final int nReduce;

    private static final String INPUT_DIR = "input";
    private static final String TMP_DIR = "tmp";
    private static final String OUTPUT_DIR = "output";

    private static final Logger logger = LoggerFactory.getLogger(Worker.class);

    @Override
    public void run() {
        while (true) {
            Task task;
            try {
                task = coordinator.getTask();
                switch (task.type) {
                    case DONE -> {
                        if (allDoneCheck()) {
                            return;
                        }
                    }
                    case MAP -> handleMapTask(task);
                    case REDUCE -> handleReduceTask(task);
                    default -> {
                        logger.error("Неизвестный тип задачи: {}", task.type);
                        return;
                    }
                }
            } catch (InterruptedException | IOException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private boolean allDoneCheck() throws InterruptedException, IOException {
        if (coordinator.isDone()) {
            return true;
        }
        Thread.sleep(100);
        return false;
    }

    private void handleMapTask(Task task) {
        try {
            // Чтение содержимого файла
            Path inputPath = Paths.get(INPUT_DIR, task.filename);
            String content = Files.readString(inputPath);
            logger.info("Читаю содержимое файла {}", task.filename);

            // Применение функции Map
            List<KeyValue> kvs = MapReduce.map(task.filename, content);
            List<String> outputFiles = new ArrayList<>();

            // Создание промежуточных файлов
            Map<Integer, List<KeyValue>> partitions = new HashMap<>();

            // Разделение по задачам Reduce
            for (KeyValue kv : kvs) {
                int r = Math.abs(kv.key().hashCode()) % nReduce;
                partitions.computeIfAbsent(r, k -> new ArrayList<>()).add(kv);
            }

            // Запись разделенных данных в файлы
            for (Map.Entry<Integer, List<KeyValue>> entry : partitions.entrySet()) {
                int reduceId = entry.getKey();
                String filename = Paths.get(TMP_DIR, "mr-" + task.id + "-" + reduceId).toString();

                try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(filename))) {
                    out.writeObject(entry.getValue());
                    outputFiles.add(filename);
                    logger.info("Создаю временные файлы: {}", filename);
                }
            }
            coordinator.completeMapTask(task.id, outputFiles);
        } catch (IOException e) {
            logger.error("Ошибка при создании временных файлов");
        }
    }

    private void handleReduceTask(Task task) {
        try {
            // Чтение всех промежуточных файлов для этой задачи Reduce
            List<KeyValue> kvs = new ArrayList<>();
            for (String file : task.intermediateFiles) {
                Path filePath = Paths.get(TMP_DIR, file);
                try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(filePath.toFile()))) {
                    @SuppressWarnings("unchecked")
                    List<KeyValue> fileKvs = (List<KeyValue>) in.readObject();
                    kvs.addAll(fileKvs);
                } catch (ClassNotFoundException e) {
                    logger.error("Ошибка при попытке прочитать объект");
                }
            }
            logger.info("Читаю временные файлы относящиеся к этой задаче reduce: {}", task.id);

            // Сортировка по ключу
            kvs.sort(Comparator.comparing(KeyValue::key));

            // Группировка по ключу
            Map<String, List<String>> grouped = new HashMap<>();
            for (KeyValue kv : kvs) {
                grouped.computeIfAbsent(kv.key(), k -> new ArrayList<>()).add(kv.value());
            }

            // Запись результата в файл
            String outFile = Paths.get(OUTPUT_DIR, "mr-out-" + task.id).toString();
            try (PrintWriter writer = new PrintWriter(outFile)) {
                for (Map.Entry<String, List<String>> entry : grouped.entrySet()) {
                    String result = MapReduce.reduce(entry.getKey(), entry.getValue());
                    writer.println(entry.getKey() + " " + result);
                }
            }
            logger.info("Создаю промежуточный файл для последующего объединения {}", outFile);

            coordinator.completeReduceTask(task.id);
        } catch (IOException e) {
            logger.error("Ошибка записи файла");
        }
    }
}
