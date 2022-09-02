using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

internal static class Program
{
    static async Task Main()
    {
        var bootstrapServers = "localhost:9092";
        var schemaRegistryUrl = "http://localhost:8081";
        var topicName = "topic5";
        var dataSchema = (RecordSchema)RecordSchema.Parse(@"{
                ""type"" : ""record"",
                ""name"" : ""Data"",
                ""namespace"" : ""example"",
                ""fields"" : [ {
                  ""name"" : ""name"",
                  ""type"" : ""string""
                }, {
                  ""name"" : ""foo"",
                  ""type"" : ""string"",
                  ""default"" : """"
                }, {
                  ""name"" : ""list"",
                  ""type"" : {
                    ""type"" : ""array"",
                    ""items"" : {
                      ""type"" : ""record"",
                      ""name"" : ""Item"",
                      ""fields"" : [ {
                        ""name"" : ""name"",
                        ""type"" : ""string""
                      } ]
                    }
                  }
                } ]
            }");
            
        // Error below is caused by schema mismatch (missing namespace parameter) but doesn't explain the problem 
        // error producing message: Confluent.Kafka.ProduceException`2[System.String,Avro.Generic.GenericRecord]: Local: Value serialization error
        //      ---> Avro.AvroException: GenericRecord required to write against record schema but found Avro.Generic.GenericRecord in field list
        //      ---> Avro.AvroException: GenericRecord required to write against record schema but found Avro.Generic.GenericRecord
        var itemSchema = (RecordSchema)RecordSchema.Parse(@"{
              ""type"" : ""record"",
              ""name"" : ""Item"",
              ""namespace"":""example"",
              ""fields"" : [ {
                ""name"" : ""name"",
                ""type"" : ""string""
              } ]
            }");
            
        using var schemaRegistry =
            new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = schemaRegistryUrl });
        using var producer =
            new ProducerBuilder<string, GenericRecord>(
                    new ProducerConfig { BootstrapServers = bootstrapServers })
                .SetValueSerializer(new AvroSerializer<GenericRecord>(schemaRegistry))
                .Build();
        
        var item = new GenericRecord(itemSchema);
        item.Add("name", "Name5");
        var genericRecords = new[] { item };
            
        var data = new GenericRecord(dataSchema);
        data.Add("foo", "Foo");
        data.Add("name", "Name6");
        data.Add("list", genericRecords);

        try
        {
            var result = await producer.ProduceAsync(topicName,
                new Message<string, GenericRecord> { Key = "key1", Value = data });
                
            Console.WriteLine($"produced to: {result.TopicPartitionOffset}");
        }
        catch (ProduceException<string, GenericRecord> ex)
        {
            Console.WriteLine($"error producing message: {ex}");
        }
    }
}