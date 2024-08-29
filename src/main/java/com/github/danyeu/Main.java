package com.github.danyeu;

public class Main {
    public static void main(String[] args) throws Exception {
        RunADBC.run();
        System.out.println("\n\n###############################\n\n");
        RunJDBC.run();
        // ADBC 214241
        // JDBC
    }
}
