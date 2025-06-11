package jdev.kovalev.task;

import lombok.AllArgsConstructor;

import java.util.List;

@AllArgsConstructor
public class Task {
    public TaskType type;
    public int id;
    public String filename;
    public List<String> intermediateFiles;
}
