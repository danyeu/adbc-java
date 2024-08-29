package com.github.danyeu;

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.driver.jdbc.JdbcDriver;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.HashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws Exception {
        // Connection details
        String url = "jdbc:postgresql://localhost:5433/tpcds";
        String user = "admin";
        String password = "password";

        final Map<String, Object> parameters = new HashMap<>();
        parameters.put("uri", url);
        parameters.put("username", user);
        parameters.put("password", password);
        try (
                BufferAllocator allocator = new RootAllocator();
                AdbcDatabase db = new JdbcDriver(allocator).open(parameters);
                AdbcConnection adbcConnection = db.connect();
                AdbcStatement stmt = adbcConnection.createStatement()
        ) {
            stmt.setSqlQuery("select * from call_center limit 5;");
            AdbcStatement.QueryResult queryResult = stmt.executeQuery();

            printQueryResult(queryResult);
        } catch (AdbcException e) {
            throw new Exception(e);
        }
    }

    public static void printQueryResult(AdbcStatement.QueryResult queryResult) throws Exception {
        // Get the schema of the result set
        VectorSchemaRoot root = queryResult.getReader().getVectorSchemaRoot();

        // Print column headers
        root.getSchema().getFields().forEach(field -> System.out.print(field.getName() + "\t"));
        System.out.println();

        // Print values
        while (queryResult.getReader().loadNextBatch()) {
            System.out.println(queryResult.getReader().getVectorSchemaRoot().getFieldVectors().toString());
        }
    }
}
