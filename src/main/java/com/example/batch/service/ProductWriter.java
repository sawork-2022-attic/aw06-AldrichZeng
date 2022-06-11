package com.example.batch.service;

import com.example.batch.model.Product;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ItemWriter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static java.lang.Class.forName;

public class ProductWriter implements ItemWriter<Product>, StepExecutionListener {

    static final String USER = "root";
    static final String PASSWORD = "123456";
    static final String DB_URL = "jdbc:mysql://localhost:3306/awdb";
    static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    Connection conn;

    private void connect() throws ClassNotFoundException, SQLException {
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(DB_URL, USER, PASSWORD);
    }

    public ProductWriter() throws SQLException, ClassNotFoundException {
        connect();
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {

    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        return null;
    }

    @Override
    public void write(List<? extends Product> list) throws Exception {
        // 基于chunk的写入
        for (Product p : list) {
            Statement stmt = conn.createStatement();
            StringBuilder str = new StringBuilder();
            // 构造category字段
            for (String category : p.getCategory()) {
                str.append(category + ",");
            }
            if (str.length() == 0) {
                continue;
            }
            str.deleteCharAt(str.length() - 1);
            String categoryStr = str.toString();
            // 构造iamgeURL字段
            str = new StringBuilder();
            for (String imageURLHighRes : p.getCategory()) {
                str.append(imageURLHighRes + ",");
            }
            if(str.length()==0){
                continue;
            }
            str.deleteCharAt(str.length()-1);
            String imageStr = str.toString();



            stmt.executeUpdate("INSERT IGNORE INTO magazine (main_cat, title, asin, category, imageURLHighRes) VALUES "
                    +
                    String.format("('%s', '%s', '%s', '%s', '%s');", p.getAsin().replaceAll("\'", "\\\\'"),
                            p.getTitle().replaceAll("\'", "\\\\'"), p.getAsin().replaceAll("\'", "\\\\'"),
                            categoryStr.replaceAll("\'", "\\\\'"),
                            imageStr.replaceAll("\'", "\\\\'")));
        }
        System.out.println("chunk written");
    }
}
