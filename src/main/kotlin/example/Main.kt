package example

import com.github.avrokotlin.avro4k.Avro
import io.confluent.kafka.serializers.KafkaAvroSerializer
import kotlinx.serialization.Serializable
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Duration
import java.util.Properties

@Serializable
data class Item(val name: String)

@Serializable
data class Data(val name: String, val list: List<Item>)

fun main() {
    val schemaRegistryUrl = "http://localhost:8081"
    val bootstrapServersUrl = "localhost:9092"
    val topic = "topic5"
    val key = "key1"

    val dataAvro4KSchema = Avro.default.schema(Data.serializer())
    val itemAvro4KSchema = Avro.default.schema(Item.serializer())
    println("dataSchema = ${dataAvro4KSchema.toString(true)}")

//    val registryClient = CachedSchemaRegistryClient(schemaRegistryUrl, 10000)
//    val register = registryClient.register("$topic-value", dataSchema)
//    println("register = ${register}")

    val dataRecord1 = Avro.default.toRecord(Data.serializer(), Data("Name1", listOf(Item("Name2"))))

    val dataRecord2 = GenericRecordBuilder(dataAvro4KSchema).apply {
        set("name", "Name3")
        set("list", listOf(GenericRecordBuilder(itemAvro4KSchema).apply {
            set("name", "Name4")
        }.build()))
    }.build()

    val props = Properties()
    props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServersUrl
    props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
    props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = KafkaAvroSerializer::class.java
    props["schema.registry.url"] = schemaRegistryUrl
//    props["auto.register.schemas"] = false

    val producer = KafkaProducer<String, GenericRecord>(props)
    try {
        producer.send(ProducerRecord(topic, key, dataRecord1))
        producer.send(ProducerRecord(topic, key, dataRecord2))
    } finally {
        producer.flush()
        producer.close()
    }

    // Consumer
    val consumerProps = Properties()
    consumerProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServersUrl
    consumerProps[ConsumerConfig.GROUP_ID_CONFIG] = "group1"
    consumerProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringDeserializer"
    consumerProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = "io.confluent.kafka.serializers.KafkaAvroDeserializer"
    consumerProps[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
    consumerProps["schema.registry.url"] = schemaRegistryUrl

    val consumer: Consumer<String, GenericRecord> = KafkaConsumer(consumerProps)
    consumer.subscribe(listOf(topic))
    consumer.use {
        while (true) {
            val records = it.poll(Duration.ofMillis(100))
            for (record in records) {
                val fromRecord = Avro.default.fromRecord(Data.serializer(), record.value())
                System.out.printf("$fromRecord (offset = %d, key = %s, value = %s) \n", record.offset(), record.key(), record.value())
            }
        }
    }
}
