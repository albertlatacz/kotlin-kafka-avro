package example

import com.github.avrokotlin.avro4k.Avro
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Arrays
import java.util.Properties


@Serializable
data class Item(val name: String)

@Serializable
data class Data(val name: String, val list: List<Item>)


fun main() {
    val topic = "topic4"
    val schemaRegistryUrl = "http://localhost:8081"
    val bootstrapServersUrl = "localhost:9092"

    val registryClient = CachedSchemaRegistryClient(schemaRegistryUrl, 10000)

    val dataSchema = Avro.default.schema(Data.serializer())
    println("dataSchema = ${dataSchema.toString(true)}")

    val register = registryClient.register(topic, dataSchema)
    println("register = ${register}")

    val dataRecord = Avro.default.toRecord(Data.serializer(), Data("Name1", listOf(Item("Name2"))))

    val props = Properties()
    props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServersUrl
    props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
    props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = KafkaAvroSerializer::class.java
    props["schema.registry.url"] = schemaRegistryUrl
    props["auto.register.schemas"] = false

    val producer = KafkaProducer<String, GenericRecord>(props)

    val key = "key1"
    val record = ProducerRecord(topic, key, dataRecord)


    try {
        producer.send(record)
    } catch (e: SerializationException) {
        // may need to do something with it
    } // When you're finished producing records, you can flush the producer to ensure it has all been written to Kafka and
    // then close the producer to free its resources.
    finally {
        producer.flush()
        producer.close()
    }

    // Consumer
    val props2 = Properties()

    props2[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServersUrl
    props2[ConsumerConfig.GROUP_ID_CONFIG] = "group1"


    props2[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringDeserializer"
    props2[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = "io.confluent.kafka.serializers.KafkaAvroDeserializer"
    props2["schema.registry.url"] = schemaRegistryUrl

    props2[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"

    val consumer: Consumer<String, GenericRecord> = KafkaConsumer(props2)
    consumer.subscribe(Arrays.asList(topic))

    try {
        while (true) {
            val records = consumer.poll(100)
            for (record in records) {
                System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value())
            }
        }
    } finally {
        consumer.close()
    }
}
