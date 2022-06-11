package com.example.batch.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ItemReader;

/**
 * 该类是一个Reader
 */
public class JsonReader implements StepExecutionListener, ItemReader<JsonNode> {

    private String filename;

    private BufferedReader reader;

    private ObjectMapper objectMapper;

    public JsonReader(String filename) {
        this.filename = filename;

    }

    private void initReader() throws FileNotFoundException {
        // 加载文件
        ClassLoader classLoader = this.getClass().getClassLoader();
//        File file = new File(classLoader.getResource(filename).getFile());
        File file = new File(filename);
        reader = new BufferedReader(new FileReader(file));
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        // todo: 该步骤在何时执行？
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {

        return null;
    }

    @Override
    public JsonNode read() throws Exception {
        // todo:该方法在何时调用？Reader的执行流程是什么样的？
        if (reader == null) {
            initReader();
        }
        // 读取一行
        String line = reader.readLine();
        if (line != null) {
            return objectMapper.readTree(reader.readLine());
        } else {
            return null;
        }


    }
}
