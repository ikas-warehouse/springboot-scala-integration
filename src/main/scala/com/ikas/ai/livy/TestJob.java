package com.ikas.ai.livy;

import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;

/**
 * @author tang.xuandong
 * @version 1.0.0
 * @date 2023/8/4 14:41
 */
public class TestJob {

    public static void main(String[] args) throws IOException, URISyntaxException, ExecutionException, InterruptedException {

        String livyUrl = "";
        String piJar = null;
        int samples = 1;

        LivyClient client = new LivyClientBuilder()
                .setURI(new URI(livyUrl))
                .build();

        try {
            System.err.printf("Uploading %s to the Spark context...\n", piJar);
            client.uploadJar(new File(piJar)).get();

            System.err.printf("Running PiJob with %d samples...\n", samples);
            double pi = client.submit(new PiJob(samples)).get();

            System.out.println("Pi is roughly: " + pi);
        } finally {
            client.stop(true);
        }
    }
}
