package com.github.danyeu;

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.driver.jdbc.JdbcDriver;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;


public class RunADBC {

    public static void run() {
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
            String directory = "/home/daniel/IdeaProjects/adbc-java/src/main/resources/queries";
            long startTime = System.currentTimeMillis();
            System.out.println("start time " + startTime);

            for (int i = 1; i < 100; i++) {
                if (i == 4) {
                    continue;
                }
                String filepath = String.format("%s/query_%d.sql", directory, i);
                String query = Files.readString(Paths.get(filepath), StandardCharsets.UTF_8);
                stmt.setSqlQuery(query);
                long queryStart = System.currentTimeMillis();
                try (AdbcStatement.QueryResult queryResult = stmt.executeQuery()) {
                    long queryDuration = System.currentTimeMillis() - queryStart;
                    queryResult.close();
                    System.out.printf("Query %d: %d\n", i, queryDuration);
                } catch (Exception e) {
                    System.out.println("ERROR (q" + i + "): " + e.getMessage());
                    continue;
                }
            }
            long totalDuration = System.currentTimeMillis() - startTime;
            System.out.printf("Total: %d\n", totalDuration);
        } catch (Exception e) {
            System.out.println("ERROR: " + e);
        }
    }
}
