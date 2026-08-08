package com.kd.teams.scaler.listeners;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kd.teams.scaler.dto.FileProcessEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Slf4j
@Component
public class InboundListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(InboundListener.class);

    private final ObjectMapper objectMapper = new ObjectMapper();
    @Value("${spark.home}")
    private String sparkHome;
    @Value("${spark.deploy.mode}")
    private String deployMode;
    @Value("${spark.master}")
    private String master;
    @Value("${spark.image}")
    private String image;
    @Value("${spark.s3.url}")
    private String s3Url;
    @Value("${spark.catalog.url}")
    private String catalogUrl;
    @Value("${ignite.endpoint}")
    private String igniteEndpoint;
    @Value("${spark.appname}")
    private String appName;

    @Autowired
    private KafkaTemplate<String, FileProcessEvent> kafkaTemplate;

    @Autowired
    private ApplicationContext context;

    @KafkaListener(topics = "rustfs.inbound.upload.topic", groupId = "keda-inbound-trigger-group",
            autoStartup = "${kafka.listener.inbound.enabled:true}" ,
            concurrency = "1")
    public void listenInbound(String rawJson, Acknowledgment ack) throws IOException {
        JsonNode root = objectMapper.readTree(rawJson);
        String mainClass = "com.kd.ImportData";
        System.out.println("Received: ");
        JsonNode recordsArray = root.path("Records");
        if (recordsArray.isArray() && !recordsArray.isEmpty()) {
            JsonNode firstRecord = recordsArray.get(0);

            String actualFileName = firstRecord.path("s3").path("object").path("key").asText();
            long fileSize = firstRecord.path("s3").path("object").path("size").asLong();
            String bucketName = firstRecord.path("s3").path("bucket").path("name").asText();

            log.info("--- Extracted target path details ---");
            log.info("Bucket: {}{}", "\t".repeat(3), bucketName);
            log.info("File Name: {}{} ", "\t".repeat(3), actualFileName);
            log.info("Size: {}{} bytes", "\t".repeat(3), fileSize);
            log.info("--- Environment Variables ---");
            log.info("Spark Home: {}{}", "\t".repeat(3), sparkHome);
            log.info("Deploy Mode: {}{}", "\t".repeat(3), deployMode);
            log.info("Main Class: {}{}", "\t".repeat(3), mainClass);
            log.info("Master: {}{}", "\t".repeat(3), master);
            log.info("Image: {}{}", "\t".repeat(3), image);
            log.info("S3 URL: {}{}", "\t".repeat(3), s3Url);
            log.info("Catalog URL: {}{}", "\t".repeat(3), catalogUrl);
            log.info("App Name: {}{}", "\t".repeat(3), appName);

            // submit the spark job to k8s cluster
            SparkLauncher launcher = new SparkLauncher()
                    .setAppName(appName)
                    .setMaster(master)
                    .setDeployMode(deployMode)
                    .setSparkHome(sparkHome)
                    .setMainClass(mainClass)
                    .setAppResource("local:///opt/spark/work/ignite-spark-1.0.jar")
                    .setConf("spark.executor.instances", "1")
                    .setConf("spark.kubernetes.executor.deleteOnTermination", "false")
                    .setConf("spark.kubernetes.container.image", image)
                    .setConf("spark.kubernetes.container.image.pullPolicy", "IfNotPresent")
                    .setConf("spark.kubernetes.namespace", "spark")
                    .setConf("spark.kubernetes.authenticate.driver.serviceAccountName", "spark-sa")
                    .setConf("spark.kubernetes.driverEnv.SPARK_CLASS", mainClass)
                    .setConf("spark.kubernetes.driverEnv.S3_URL", s3Url)
                    .setConf("spark.kubernetes.driverEnv.CATALOG_URL", catalogUrl)
                    .setConf("spark.kubernetes.driverEnv.FILE_NAME", actualFileName)
                    .setConf("spark.kubernetes.driverEnv.APP_NAME", appName)
                    .setConf("spark.kubernetes.file.upload.path", "local:///tmp");

            SparkAppHandle handle = launcher.startApplication();

            // attach a listener to get the status of the spark job
            // if error in the spark job? exit with failure so scaledJob can handle retry : exit with success
            handle.addListener(new SparkAppHandle.Listener() {
                @Override
                public void stateChanged(SparkAppHandle h) {
                    log.info("Logging the state {}", h.getState());
                    log.info("Logging the app id {}", h.getAppId());

                    h.getError()
                            .ifPresentOrElse(error -> {
                                        ack.acknowledge();
                                        log.error("unable to process the file: {}, due to error: {}", actualFileName, error.getMessage());

                                        FileProcessEvent fileProcessEvent = new FileProcessEvent(actualFileName, "FAILURE");
                                        ProducerRecord<String, FileProcessEvent> fileProcessEventProducerRecord = new ProducerRecord<>("inbound.spark.job.failure", fileProcessEvent);
                                        kafkaTemplate.send(fileProcessEventProducerRecord);

                                        log.info("sending acknowledgement after failure");
                                        initiateGracefulShutdown(0);
                                    },
                                    () -> {
                                        ack.acknowledge();
                                        log.info("file processes successfully for file {}", actualFileName);
                                        FileProcessEvent fileProcessEvent = new FileProcessEvent(actualFileName, "SUCCESS");
                                        ProducerRecord<String, FileProcessEvent> fileProcessEventProducerRecord = new ProducerRecord<>("inbound.spark.job.success", fileProcessEvent);
                                        kafkaTemplate.send(fileProcessEventProducerRecord);
                                        initiateGracefulShutdown(0);
                                    });
                }

                @Override
                public void infoChanged(SparkAppHandle h) {
                }
            });

        }
    }


    @KafkaListener( topics = "iceberg.raw.created.topic", groupId = "keda-inbound-trigger-group",
            autoStartup = "${kafka.listener.iceberg.enabled:true}" ,
            concurrency = "1")
    public void listenInputCreated(String rawJson, Acknowledgment ack) throws IOException {
        JsonNode root = objectMapper.readTree(rawJson);

        String mainClass = "com.kd.EnrichData";
        System.out.println("Received: ");
        JsonNode recordsArray = root.path("Key");
        String s = recordsArray.asText();
        log.info("--- Extracted target path details ---");
        log.info("Processing snapshot: {}{}", "\t".repeat(3), s);
        log.info("--- Environment Variables ---");
        log.info("Spark Home: {}{}", "\t".repeat(3), sparkHome);
        log.info("Deploy Mode: {}{}", "\t".repeat(3), deployMode);
        log.info("Main Class: {}{}", "\t".repeat(3), mainClass);
        log.info("Master: {}{}", "\t".repeat(3), master);
        log.info("Image: {}{}", "\t".repeat(3), image);
        log.info("S3 URL: {}{}", "\t".repeat(3), s3Url);
        log.info("Catalog URL: {}{}", "\t".repeat(3), catalogUrl);
        log.info("App Name: {}{}", "\t".repeat(3), appName);

        // submit the spark job to k8s cluster
        SparkLauncher launcher = new SparkLauncher()
                .setAppName(appName)
                .setMaster(master)
                .setDeployMode(deployMode)
                .setSparkHome(sparkHome)
                .setMainClass(mainClass)
                .setAppResource("local:///opt/spark/work/ignite-spark-1.0.jar")
                .setConf("spark.executor.instances", "1")
                .setConf("spark.kubernetes.executor.deleteOnTermination", "false")
                .setConf("spark.kubernetes.container.image", image)
                .setConf("spark.kubernetes.container.image.pullPolicy", "IfNotPresent")
                .setConf("spark.kubernetes.namespace", "spark")
                .setConf("spark.kubernetes.authenticate.driver.serviceAccountName", "spark-sa")
                .setConf("spark.kubernetes.driverEnv.SPARK_CLASS", mainClass)
                .setConf("spark.kubernetes.driverEnv.S3_URL", s3Url)
                .setConf("spark.kubernetes.driverEnv.CATALOG_URL", catalogUrl)
                .setConf("spark.kubernetes.driverEnv.IGNITE_ENDPOINT", igniteEndpoint)
                .setConf("spark.kubernetes.driverEnv.APP_NAME", appName)
                .setConf("spark.kubernetes.file.upload.path", "local:///tmp");


        SparkAppHandle handle = launcher.startApplication();

        handle.addListener(new SparkAppHandle.Listener() {
            @Override
            public void stateChanged(SparkAppHandle h) {
                log.info("Logging the state {}", h.getState());
                log.info("Logging the app id {}", h.getAppId());

                h.getError()
                        .ifPresentOrElse(error -> {
                                    ack.acknowledge();
                                    log.info("sending acknowledgement after error");
                                    initiateGracefulShutdown(0);
                                },
                                () -> {
                                    ack.acknowledge();
                                    log.info("sending acknowledgement after success");
                                    initiateGracefulShutdown(0);
                                });
            }

            @Override
            public void infoChanged(SparkAppHandle h) {
            }
        });


    }

    private void initiateGracefulShutdown(int exitCode) {
        LOGGER.info("Initiating graceful shutdown sequence with exit code: {}", exitCode);
        int systemExitCode = SpringApplication.exit(context, () -> exitCode);
        System.exit(systemExitCode);
    }

}
