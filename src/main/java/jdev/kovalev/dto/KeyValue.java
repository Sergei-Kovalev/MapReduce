package jdev.kovalev.dto;

import java.io.Serializable;

public record KeyValue(String key, String value) implements Serializable {}
