package jdev.kovalev;

import jdev.kovalev.task.Task;
import jdev.kovalev.task.TaskType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class Coordinator {
    /** Список входных файлов. */
    private final List<String> inputFiles;
    /** Количество reduce-задач. */
    private final int nReduce;
    /** Очередь для задач map, которые нужно обработать. */
    private final Queue<Integer> mapTasks;
    /** Очередь для задач reduce, которые нужно обработать. */
    private final Queue<Integer> reduceTasks;
    /** Мапа со статусами map задач. */
    private final Map<Integer, Boolean> mapTaskStatus;
    /** Мапа со статусами reduce задач. */
    private final Map<Integer, Boolean> reduceTaskStatus;
    /** Мапа - хранит id map задачи : список файлов где хранятся промежуточные файлы. */
    private final Map<Integer, List<String>> mapTaskOutputs;
    /** Флаг, что все задачи map выполнены. */
    private boolean allMapDone;
    /** Флаг, что все задачи reduce выполнены. */
    private boolean allReduceDone;
    /** Флаг записи результата */
    private boolean outputFileExists;

    private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);

    static {
        try {
            MapReduce.clearTmpAndOutputDirectories();
        } catch (IOException e) {
            logger.error("Ошибка при очистке директорий");
            throw new RuntimeException(e);
        }
    }

    public Coordinator(List<String> inputFiles, int nReduce) {
        this.inputFiles = inputFiles;
        this.nReduce = nReduce;
        this.mapTasks = new LinkedList<>();
        this.reduceTasks = new LinkedList<>();
        this.mapTaskStatus = new HashMap<>();
        this.reduceTaskStatus = new HashMap<>();
        this.mapTaskOutputs = new HashMap<>();
        this.allMapDone = false;
        this.allReduceDone = false;
        this.outputFileExists = false;

        // Инициализация задач Map
        for (int i = 0; i < inputFiles.size(); i++) {
            mapTasks.add(i);
            mapTaskStatus.put(i, false);
        }

        // Инициализация задач Reduce
        for (int i = 0; i < nReduce; i++) {
            reduceTasks.add(i);
            reduceTaskStatus.put(i, false);
        }
    }

    public synchronized Task getTask() throws InterruptedException, IOException {
        // Сначала раздаем задачи Map
        if (!mapTasks.isEmpty() && !allMapDone) {
            int taskId = mapTasks.poll();
            return new Task(TaskType.MAP, taskId, inputFiles.get(taskId), null);
        }

        // Если все задачи Map назначены, но не завершены - ждем
        if (!allMapDone) {
            boolean allDone = mapTaskStatus.values().stream().allMatch(done -> done);
            if (allDone) {
                allMapDone = true;
                notifyAll();
            } else {
                wait();
                return getTask();
            }
        }

        // Затем раздаем задачи Reduce
        if (!reduceTasks.isEmpty()) {
            int reduceId = reduceTasks.poll();
            // Собираем промежуточные файлы для этой задачи Reduce
            List<String> files = new ArrayList<>();
            for (int mapId : mapTaskOutputs.keySet()) {
                files.add("mr-" + mapId + "-" + reduceId);
            }
            return new Task(TaskType.REDUCE, reduceId, null, files);
        }

        // Проверяем завершение всех задач Reduce
        boolean allDone = reduceTaskStatus.values().stream().allMatch(done -> done);
        if (allDone) {
            allReduceDone = true;
            notifyAll();
            if (!outputFileExists) {
                MapReduce.mergeReduceOutputs(nReduce);
                outputFileExists = true;
            }
        }
        return new Task(TaskType.DONE, -1, null, null);
    }

    public synchronized void completeMapTask(int taskId, List<String> outputs) {
        mapTaskStatus.put(taskId, true);
        mapTaskOutputs.put(taskId, outputs);
        notifyAll();
    }

    public synchronized void completeReduceTask(int taskId) {
        reduceTaskStatus.put(taskId, true);
        notifyAll();
    }

    public synchronized boolean isDone() {
        return allReduceDone;
    }
}
