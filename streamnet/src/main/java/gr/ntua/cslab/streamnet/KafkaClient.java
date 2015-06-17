package gr.ntua.cslab.streamnet;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaClient {
	private static Properties props;
	private static final String TOPIC_NAME = "sflows";
	private static final String FILENAME = 
			"/home/dimitris/sflow.2014-02-17_17-15-36.3600sec.pcap.gz";
	
	private static void loadProperties() {
		props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.RETRIES_CONFIG, "3");
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 200);
		props.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, true);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
				"org.apache.kafka.common.serialization.StringSerializer");
	}
	
	public static void main(String[] args) throws Exception {
		loadProperties();
		
		KafkaProducer<String, String> kp = new KafkaProducer<String, String>(props);
		
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(
	        		new GZIPInputStream(new FileInputStream(FILENAME)), "UTF-8"));
			String line;
			line = br.readLine();
			while (line != null) {
				ProducerRecord<String, String> data = 
						new ProducerRecord<String, String>(TOPIC_NAME, 0, "0", line);
				kp.send(data);
				System.out.println("Sending record: " + line);
				line = br.readLine();
			}
			br.close();
		    kp.close();
		} catch (IOException e) {
			System.err.println("Failed: Could not read sFlow file!");
		}
    }
}
