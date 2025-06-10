package jdev.kovalev;

import jdev.kovalev.dto.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class MapReduce {
    private static final String TMP_DIR = "tmp";
    private static final String OUTPUT_DIR = "output";

    private static final Logger logger = LoggerFactory.getLogger(MapReduce.class);
    public static final String FINAL_FILE_NAME = "mr-out-final.txt";

    // Функция Map - разделяет текст на слова
    public static List<KeyValue> map(String fileName, String content) {
        String[] words = content.split("\\s+");
        return Arrays.stream(words)
                .filter(word -> !word.isEmpty())
                .map(word -> new KeyValue(word, "1"))
                .collect(Collectors.toList());
    }

    // Функция Reduce - подсчитывает количество значений для каждого ключа
    public static String reduce(String key, List<String> values) {
        return String.valueOf(values.size());
    }

    // Функция записи
    public static void mergeReduceOutputs(int nReduce) throws IOException {
        Map<String, String> finalResult = new TreeMap<>();

        for (int i = 0; i < nReduce; i++) {
            Path filePath = Paths.get(OUTPUT_DIR, "mr-out-" + i);
            if (Files.exists(filePath)) {
                readReduceFile(finalResult, filePath);
            }
        }
        // Запись финального результата
        createOutputFile(finalResult);
    }

    // Функция очистки директорий output и tmp
    public static void clearTmpAndOutputDirectories() throws IOException {
        deleteDirectory(TMP_DIR);
        deleteDirectory(OUTPUT_DIR);
        Files.createDirectories(Paths.get(TMP_DIR));
        Files.createDirectories(Paths.get(OUTPUT_DIR));
    }

    private static void readReduceFile(Map<String, String> finalResult, Path filePath) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath.toFile()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(" ", 2);
                if (parts.length == 2) {
                    finalResult.put(parts[0], parts[1]);
                }
            }
        }
        logger.info("Файл {} прочтён для объединения результата", filePath);
    }

    private static void createOutputFile(Map<String, String> finalResult) throws IOException {
        Files.createDirectories(Paths.get(OUTPUT_DIR));
        String outFile = Paths.get(OUTPUT_DIR, FINAL_FILE_NAME).toString();
        try (PrintWriter writer = new PrintWriter(new FileWriter(outFile))) {
            for (Map.Entry<String, String> entry : finalResult.entrySet()) {
                writer.println(entry.getKey() + " " + entry.getValue());
            }
        }
        logger.info("Результат записан в директорию {}. Название файла - {}", OUTPUT_DIR, FINAL_FILE_NAME);
    }

    private static void deleteDirectory(String directoryName) throws IOException {
        Path directoryPath = Paths.get(directoryName);
        if (Files.exists(directoryPath)) {
            deleteDirectoryAndFiles(directoryPath);
            logger.info("Директория {} очищена перед работой.", directoryName);
        } else {
            logger.info("Директория {} не существует.", directoryName);
        }
    }

    private static void deleteDirectoryAndFiles(Path path) throws IOException {
        if (Files.exists(path)) {
            Files.walkFileTree(path, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        }
    }
}