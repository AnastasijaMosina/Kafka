using Confluent.Kafka;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaConnector.Consumers
{
    internal class AdvancedKafkaConsumer
    {
        private readonly ConsumerConfig _config;

        public AdvancedKafkaConsumer()
        {
            _config = new ConsumerConfig
            {
                GroupId = "advanced-consumer-group",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
                SessionTimeoutMs = 6000,
                MaxPollIntervalMs = 300000
            };
        }

        public async Task StartConsuming(string topicName)
        {
            Console.WriteLine($"Advanced consumer listening to '{topicName}' topic on localhost:9092");
            Console.WriteLine("Press Ctrl+C to exit");
            Console.WriteLine();

            Console.WriteLine("=== ADVANCED CONSUMER CONFIGURATION ===");
            foreach (var prop in _config.GetType().GetProperties())
            {
                if (prop.CanRead) // Ensure the property has a get accessor
                {
                    var value = prop.GetValue(_config, null);
                    if (value != null)
                        Console.WriteLine($"{prop.Name}: {value}");
                }
            }
            Console.WriteLine();

            using (var consumer = new ConsumerBuilder<string, string>(_config).Build())
            {
                consumer.Subscribe(topicName);

                var cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true;
                    cts.Cancel();
                };

                try
                {
                    Console.WriteLine("Advanced consumer started. Waiting for messages...");
                    while (!cts.Token.IsCancellationRequested)
                    {
                        try
                        {
                            var cr = consumer.Consume(cts.Token);

                            // Print all message details in a single line
                            Console.WriteLine(
                                $"[Advanced] Key: {cr.Message.Key ?? "(null)"} | Value: {cr.Message.Value} | Partition: {cr.Partition} | Offset: {cr.Offset} | Timestamp: {cr.Message.Timestamp.UtcDateTime:O}");

                            // Simulate processing time
                            await Task.Delay(100);

                            // Manually commit offset
                            consumer.Commit(cr);
                            Console.WriteLine($"[Advanced] Message processed successfully | Manually committed offset {cr.Offset}");
                            Console.WriteLine();
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Consume error: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("Shutting down advanced consumer...");
                }
                finally
                {
                    consumer.Close();
                    Console.WriteLine("Advanced consumer closed.");
                }
            }
        }
    }
}

