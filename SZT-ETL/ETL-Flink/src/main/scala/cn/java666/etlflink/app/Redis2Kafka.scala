package cn.java666.etlflink.app

import java.util.Properties
import cn.java666.etlflink.source.MyRedisSourceFun
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig

import scala.util.Random

/**
 * @author Geek
 * @date 2020-04-14 04:35:36
 *
 * redis szt:pageJson 抽取源数据到 kafka
 *
 */
object Redis2Kafka {
	def main(args: Array[String]): Unit = {
		val env = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		
		val prop = new Properties
		//prop.load(ClassLoader.getSystemResourceAsStream("kafka.properties"))
		prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
//		prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
//		prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
//		prop.put(ProducerConfig.ACKS_CONFIG, "all")
//		prop.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432")
//		prop.put(ProducerConfig.RETRIES_CONFIG, "0")
//		prop.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "300")
		prop.put(ProducerConfig.BATCH_SIZE_CONFIG, "5")
		prop.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000") //TimeoutException: Topic not present in metadata after 1000 ms.
		
		val s = env.addSource[String](new MyRedisSourceFun)
			.map(x => {
				// TODO 假装休息一会，如果客户觉得速度太慢，可以加钱优化！！！但是这里我们真的需要休息，模拟流式数据连续的注入
				Thread.sleep(Random.nextInt(500))
				x
			})

		s.addSink(
			new FlinkKafkaProducer("topic-flink-szt-2022", new SimpleStringSchema(), prop)
		)
		
		env.execute("Redis2Kafka")
	}
}
