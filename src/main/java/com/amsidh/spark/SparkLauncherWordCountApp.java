package com.amsidh.spark;

import com.amsidh.spark.exception.ISRRunnable;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;

public class SparkLauncherWordCountApp {

    public static void main(String[] args) {
        SparkLauncher sparkLauncher = getSparkLauncher();
        sparkLauncher.setSparkHome("F:\\spark-2.4.3-bin-hadoop2.7");
        try {
            Process process = sparkLauncher.launch();
            System.out.println("WorldCount! SparkJob is executing.");
            new Thread(new ISRRunnable(process.getErrorStream())).start();
            int exitCode = process.waitFor();
            System.out.println("Finished! Exit code is "  + exitCode);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public static SparkLauncher getSparkLauncher() {
        SparkLauncher sparkLauncher = new SparkLauncher()
                .setVerbose(true)
                .setMaster("yarn") //master value is local or yarn
                .setDeployMode("cluster")
                .setAppName("WordCount")
                .setMainClass("com.amsidh.spark.WordCount")
                //.setAppResource("hdfs://localhost:9000/spark/apps/Spark-Example/WordCount/WordCountExample.jar")  //Application Jar Path
                .setAppResource("file:/D:/spark-apps/WordCount/WordCountExample.jar")
                .addAppArgs("hdfs://localhost:9000/spark/apps/WordCount/input/input.txt", "hdfs://localhost:9000/spark/apps/WordCount/output")
                ;
        return sparkLauncher;
    }

}
