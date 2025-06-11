package jdev.kovalev.util;

import java.nio.file.Path;
import java.nio.file.Paths;

public class PathManager {
    private static final String INPUT_DIR = "input";
    private static final String TMP_DIR = "tmp";
    private static final String OUTPUT_DIR = "output";


    public static Path getInputPath(String filename) {
        return Paths.get(INPUT_DIR, filename);
    }

    public static Path getTmpPath() {
        return Paths.get(TMP_DIR);
    }

    public static Path getTmpPath(String filename) {
        return Paths.get(TMP_DIR, filename);
    }

    public static Path getOutputPath() {
        return Paths.get(OUTPUT_DIR);
    }

    public static Path getOutputPath(String filename) {
        return Paths.get(OUTPUT_DIR, filename);
    }

    public static String getTmpDir() {
        return TMP_DIR;
    }

    public static String getOutputDir() {
        return OUTPUT_DIR;
    }
}
