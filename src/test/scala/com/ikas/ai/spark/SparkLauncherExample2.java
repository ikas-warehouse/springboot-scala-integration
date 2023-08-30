package com.ikas.ai.spark;

// 常见的方法：
// spark-submit --class com.learn.spark.SimpleApp --master yarn --deploy-mode client
// --driver-memory 2g --executor-memory 2g --executor-cores 3 ../spark-demo.jar

import com.ikas.ai.utils.InputStreamReaderRunnable;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class SparkLauncherExample2 {

    public static void main(String[] args) throws IOException, InterruptedException {

        Map<String, String> env = new HashMap();
        env.put("HADOOP_CONF_DIR", "/usr/local/hadoop/etc/overriterHaoopConf");
        env.put("JAVA_HOME", "/usr/local/java/jdk1.8.0_151");

        SparkLauncher handle = new SparkLauncher(env)
                .setSparkHome("/usr/local/spark")
                .setAppResource("/usr/local/spark/spark-demo.jar")

                .setMainClass("com.learn.spark.SimpleApp")

                .setMaster("yarn")
                .setDeployMode("cluster")
                .setConf("spark.app.id", "11222")
                .setConf("spark.driver.memory", "2g")
                .setConf("spark.akka.frameSize", "200")
                .setConf("spark.executor.memory", "1g")
                .setConf("spark.executor.instances", "32")
                .setConf("spark.executor.cores", "3")
                .setConf("spark.default.parallelism", "10")
                .setConf("spark.driver.allowMultipleContexts", "true")
                .setVerbose(true);


        Process process = handle.launch();
        InputStreamReaderRunnable inputStreamReaderRunnable = new InputStreamReaderRunnable(process.getInputStream(), "input");
        Thread inputThread = new Thread(inputStreamReaderRunnable, "LogStreamReader input");
        inputThread.start();

        InputStreamReaderRunnable errorStreamReaderRunnable = new InputStreamReaderRunnable(process.getErrorStream(), "error");
        Thread errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader error");
        errorThread.start();

        System.out.println("Waiting for finish...");
        int exitCode = process.waitFor();
        System.out.println("Finished! Exit code:" + exitCode);

    }
}