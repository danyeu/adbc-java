package com.github.danyeu;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;

public class RunJDBC {

    public static void run() {
        String url = "jdbc:postgresql://localhost:5433/tpcds";
        String user = "admin";
        String password = "password";

        try {
            Class.forName("org.postgresql.Driver");

            Connection connection = DriverManager.getConnection(url, user, password);
            Statement stmt = connection.createStatement();

            String directory = "/home/daniel/IdeaProjects/adbc-java/src/main/resources/queries";
            long startTime = System.currentTimeMillis();
            System.out.println("start time " + startTime);

            for (int i = 1; i < 100; i++) {
                if (i == 4) {
                    continue;
                }
                String filepath = String.format("%s/query_%d.sql", directory, i);
                String query = Files.readString(Paths.get(filepath), StandardCharsets.UTF_8);
                long queryStart = System.currentTimeMillis();

                try (ResultSet resultSet = stmt.executeQuery(query)) {
                    long queryDuration = System.currentTimeMillis() - queryStart;
                    resultSet.close();
                    System.out.printf("Query %d: %d\n", i, queryDuration);
                } catch (SQLException e) {
                    System.out.println("ERROR (q" + i + "): " + e.getMessage());
                    continue;
                }
            }
            long totalDuration = System.currentTimeMillis() - startTime;
            System.out.printf("Total: %d\n", totalDuration);
        } catch (Exception e) {
            System.out.println("ERROR: " + e.getMessage());
        }
    }
}
