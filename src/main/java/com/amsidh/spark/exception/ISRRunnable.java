package com.amsidh.spark.exception;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class ISRRunnable implements Runnable {
    private final BufferedReader reader;

    private ISRRunnable(BufferedReader reader) {
        this.reader = reader;
    }

    public ISRRunnable(InputStream inputStream) {
        this(new BufferedReader(new InputStreamReader(inputStream)));
    }

    public void run() {
        String line = null;
        try {
            line = reader.readLine();
            while (line != null) {
                System.out.println(line);
                line = reader.readLine();
            }
            reader.close();
        } catch (IOException e) {
            System.out.println("There is a exception when getting log: "+ e.getLocalizedMessage());
            e.printStackTrace();
            throw new RuntimeException("There is a exception when getting log.");
        }
    }
}
