package com.amsidh.spark;

import com.amsidh.spark.exception.ISRRunnable;
import com.amsidh.spark.model.Employee;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SparkLauncherDisplayAppArgsApp {

    public static void main(String[] args) {
        SparkLauncher sparkLauncher = getSparkLauncher();
        sparkLauncher.setSparkHome("F:\\spark-2.4.3-bin-hadoop2.7");
        try {
            Process process = sparkLauncher.launch();
            System.out.println("DisplayAppArgs! SparkJob is executing.");
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
        Employee employee = new Employee();

        SparkLauncher sparkLauncher = new SparkLauncher()
                .setVerbose(true)
                .setMaster("yarn") //master value is local or yarn
                .setDeployMode("cluster")
                .setAppName("DisplaySparkAppArgument")
                .setMainClass("com.amsidh.spark.DisplaySparkAppArgument")
                .setAppResource("file:/D:/spark-apps/DisplayAppArgs/DisplaySparkAppArgument.jar")
                .addAppArgs("hdfs://localhost:9000/spark/apps/WordCount/input/input.txt", loadEmployee(employee))
                ;
        return sparkLauncher;
    }

    public static String loadEmployee(Employee employee){
        employee.setName("EmployeeName1");
        employee.setEmailId("EmployeeEmailID1");
        employee.setExtraData("");
        Map<String,String> extraDataMap = new HashMap<>();
        String key="extraData";
        String value="dshg43trrhiobjdfobiuegibvbdbkdshu32324543ifhdsonyu3253bdewwe2";
        extraDataMap.put(key,value);

        employee.setExtraData(extraDataMap.get(key));
        String employeeJson = employee.toString();
        System.out.println("Employee Json Object passed to spark job: ");
        System.out.println(employeeJson);
        return employeeJson;

    }

}
