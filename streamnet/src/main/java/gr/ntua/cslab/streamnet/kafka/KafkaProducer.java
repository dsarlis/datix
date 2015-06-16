package gr.ntua.cslab.streamnet.kafka;

import java.util.concurrent.CountDownLatch;
import java.util.zip.GZIPInputStream;

import com.google.common.io.Files;

import kafka.admin.CreateTopicCommand;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

import org.apache.log4j.Logger;

public class KafkaProducer {
	private static final Logger logger = Logger.getLogger(KafkaProducer.class);
	private KafkaServer kafkaServer = null;
    private final String topicName;
    private final String kafkaHost;
    private final String fileName;


    CountDownLatch topologyStartedLatch;
    public CountDownLatch producerFinishedInitialBatchLatch = new CountDownLatch(1);


    Producer<String, String> producer;

    public KafkaProducer(String topicName, CountDownLatch topologyStartedLatch, String kafkaHost, String fileName) {
        this.topicName = topicName;
        this.topologyStartedLatch = topologyStartedLatch;
        this.kafkaHost = kafkaHost;
        this.fileName = fileName;
    }

    public void emitBatch() {
        Properties props = new Properties();
        props.put("metadata.broker.list", kafkaHost);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);

        try {
        BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(fileName)), "UTF-8"));
		String line;
			line = br.readLine();
			while (line != null) {
				KeyedMessage<String, String> data =
						new KeyedMessage<String, String>(topicName, line);
				producer.send(data);
				line = br.readLine();
			}
			br.close();
	        producer.close();
		} catch (IOException e) {
			logger.error("Failed: Could not read sFlows file!");
		}
    }

    public void createTopic(String replicaNo, String partitionNo,  String topicName) {
        String[] arguments = new String[8];
        arguments[0] = "--zookeeper";
        arguments[1] = "localhost:2000";
        arguments[2] = "--replica";
        arguments[3] = replicaNo;
        arguments[4] = "--partition";
        arguments[5] = partitionNo;
        arguments[6] = "--topic";
        arguments[7] = topicName;

        CreateTopicCommand.main(arguments);
    }

    public void startKafkaServer() {
        File tmpDir = Files.createTempDir();
        Properties props = createProperties(tmpDir.getAbsolutePath(), 9092, 1);
        KafkaConfig kafkaConfig = new KafkaConfig(props);

        kafkaServer = new KafkaServer(kafkaConfig, new MockTime());
        kafkaServer.startup();
    }

    public void shutdown() {
        kafkaServer.shutdown();
    }

    private Properties createProperties(String logDir, int port, int brokerId) {
        Properties properties = new Properties();
        properties.put("port", port + "");
        properties.put("broker.id", brokerId + "");
        properties.put("log.dir", logDir);
        properties.put("zookeeper.connect", "localhost:2000");         // Uses zookeeper created by LocalCluster
        return properties;
    }
}
