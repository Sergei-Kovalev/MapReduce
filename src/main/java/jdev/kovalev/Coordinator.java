package jdev.kovalev;

import jdev.kovalev.task.Task;
import jdev.kovalev.task.TaskType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class Coordinator {
    /** Список входных файлов. */
    private final List<String> inputFiles;
    /** Количество reduce-задач. */
    private final int nReduce;
    /** Очередь для задач map, которые нужно обработать. */
    private final BlockingQueue<Task> mapTasks;
    /** Очередь для задач reduce, которые нужно обработать. */
    private final BlockingQueue<Task> reduceTasks;
    /** Мапа - хранит id map задачи : список файлов где хранятся промежуточные файлы. */
    private final ConcurrentHashMap<Integer, List<String>> mapTaskOutputs;
    /** Флаг записи результата */
    private volatile boolean outputFileExists;
    /** Счетчик завершенных reduce задач */
    private final AtomicInteger completedReduceTasks;
    /** Флаг проверки, что reduce задачи инициализированы */
    private volatile boolean reducePhaseInitialized = false;

    private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);


    public Coordinator(List<String> inputFiles, int nReduce) {
        this.inputFiles = inputFiles;
        this.nReduce = nReduce;
        this.mapTasks = new LinkedBlockingQueue<>();
        this.reduceTasks = new LinkedBlockingQueue<>();
        this.mapTaskOutputs = new ConcurrentHashMap<>();
        this.completedReduceTasks = new AtomicInteger(0);

        // Инициализация задач Map
        for (int i = 0; i < inputFiles.size(); i++) {
            mapTasks.add(new Task(TaskType.MAP, i, inputFiles.get(i), null));
        }
        mapTasks.add(new Task(TaskType.DONE, -1, null, null));
    }

    public synchronized Task getTask() throws InterruptedException, IOException {
        // Сначала раздаем задачи Map
        Task mapTask = mapTasks.peek();
        if (mapTask != null && mapTask.type != TaskType.DONE) {
            return mapTasks.poll();
        }

        // Затем раздаем задачи Reduce
        Task reduceTask = reduceTasks.peek();
        if (reduceTask != null && reduceTask.type != TaskType.DONE) {
            return reduceTasks.poll();
        }
        return new Task(TaskType.DONE, -1, null, null);
    }

    public synchronized void completeMapTask(int taskId, List<String> outputs) {
        mapTaskOutputs.put(taskId, outputs);
        if (!reducePhaseInitialized && mapTasks.size() == 1) {
            initReduceTasks();
            reducePhaseInitialized = true;
        }
    }

    public void completeReduceTask() {
        int completed = completedReduceTasks.incrementAndGet(); // Увеличиваем счетчик
        if (completed == nReduce && !outputFileExists) {
            try {
                MapReduce.mergeReduceOutputs(nReduce);
                outputFileExists = true;
            } catch (IOException e) {
                logger.error("Ошибка при объединении результатов", e);
            }
        }
    }

    public synchronized boolean isDone() {
        return mapTasks.peek() != null && mapTasks.peek().type == TaskType.DONE
                && completedReduceTasks.get() == nReduce
                && outputFileExists;
    }

    private void initReduceTasks() {
        for (int reduceId = 0; reduceId < nReduce; reduceId++) {
            List<String> files = new ArrayList<>();
            for (int mapId : mapTaskOutputs.keySet()) {
                files.add("mr-" + mapId + "-" + reduceId);
            }
            reduceTasks.add(new Task(TaskType.REDUCE, reduceId, null, files));
        }
        reduceTasks.add(new Task(TaskType.DONE, -1, null, null));
    }

    public void returnMapTaskBack(Task task) {
        mapTasks.add(task);
    }

    public void returnReduceTaskBack(Task task) {
        reduceTasks.add(task);
    }
}
