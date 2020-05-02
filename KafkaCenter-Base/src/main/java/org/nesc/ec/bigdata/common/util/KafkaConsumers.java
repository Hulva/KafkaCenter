package org.nesc.ec.bigdata.common.util;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class KafkaConsumers<K, V>  implements Closeable{
	
	private KafkaConsumer<K, V> consumer;
	
	/**
	 *   props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConsumerProperties.getBootstrap());
     *   props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaConsumerProperties.getGroup());
     *   props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
     *   props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
     *   props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
	 */
	public KafkaConsumers (Properties consumerProps) {
		this.consumer = new KafkaConsumer<K, V>(consumerProps);
	}
	
	public KafkaConsumer<K, V> subscribe(String toppicName) {
		this.consumer.subscribe(Collections.singleton(toppicName));
		return this.consumer;
	}
	public KafkaConsumer<K, V> assign(TopicPartition topicPartition) {
		this.consumer.assign(Collections.singleton(topicPartition));
		return this.consumer;
	}
	public KafkaConsumer<K, V> seek(TopicPartition topicPartition,Long offset) {
		this.consumer.seek(topicPartition,offset);
		return this.consumer;
	}
	public ConsumerRecords<K, V> poll(Duration duration) {
		return this.consumer.poll(duration);
	}

	public void commit() {
		this.consumer.commitSync();
	}
	public void commitByPartition(Map<TopicPartition, OffsetAndMetadata> offsets){
		this.consumer.commitSync(offsets);
	}
	
	
	@Override
	public void close() throws IOException {
		this.consumer.close();
	}
}
