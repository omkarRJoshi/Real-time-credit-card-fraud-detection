package com.geca.kafka;

import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

public class Producer {

	public static void main(String[] args) {
		Logger logger = Logger.getLogger(Producer.class.getName());
		Properties properties = new Properties();
		String fileName = "/home/omkar/Desktop/transaction.csv";
		String topic = "test_transaction";
		
		
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerilizer.class);
		
		KafkaProducer<String, KafkaSchema> producer = new KafkaProducer<String, KafkaSchema>(properties);
		logger.info("Producer is created");
//		CsvReader csvReader = new CsvReader(fileName);
//		List<KafkaSchema> transactionList = csvReader.getList();
//		
//		for(KafkaSchema transaction : transactionList) {
//			ProducerRecord<String, KafkaSchema> record = new ProducerRecord<String, KafkaSchema>(topic, transaction);
//			producer.send(record);
//			try {
//				Thread.sleep(1000);
//			} catch (InterruptedException e) {
//				System.out.println("Exception while thread is sleeping .....");
//				e.printStackTrace();
//			}
//		}
		
		producer.close();
	}

}
