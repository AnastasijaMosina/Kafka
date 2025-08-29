using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaConnector.Consumers
{
    internal class BatchConsumer
    {
        private readonly ConsumerConfig _config;
        private const int BatchSize = 10;
        private const int BatchTimeoutMs = 5000;

        public BatchConsumer()
        {
            _config = new ConsumerConfig
            {
                GroupId = "batch-consumer-group",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
                MaxPartitionFetchBytes = BatchSize
            };
        }

        public async Task StartBatchConsuming()
        {
            Console.WriteLine($"Batch consumer started. Processing messages in batches of {BatchSize}...");

            var consumer = new ConsumerBuilder<string, string>(_config).Build();
            consumer.Subscribe("order-events");

            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

            var batch = new List<ConsumeResult<string, string>>();
            var lastBatchTime = DateTime.UtcNow;

            try
            {
                while (!cts.Token.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(1000));

                        if (consumeResult != null)
                        {
                            batch.Add(consumeResult);
                            Console.WriteLine($"Added message to batch. Batch size: {batch.Count}");
                        }

                        // Process batch if it's full or timeout reached
                        var shouldProcessBatch = batch.Count >= BatchSize ||
                            (batch.Count > 0 && DateTime.UtcNow.Subtract(lastBatchTime).TotalMilliseconds > BatchTimeoutMs);

                        if (shouldProcessBatch)
                        {
                            await ProcessBatch(batch);

                            // Commit all offsets in batch
                            if (batch.Count > 0)
                            {
                                consumer.Commit(batch[batch.Count - 1]);
                                Console.WriteLine($"✓ Committed batch of {batch.Count} messages");
                            }

                            batch.Clear();
                            lastBatchTime = DateTime.UtcNow;
                        }
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Consume error: {e.Error.Reason}");
                    }
                }
            }
            finally
            {
                // Process remaining messages in batch
                if (batch.Count > 0)
                {
                    await ProcessBatch(batch);
                    consumer.Commit(batch[batch.Count - 1]);
                }
                consumer.Close();
            }
        }

        private async Task ProcessBatch(List<ConsumeResult<string, string>> batch)
        {
            Console.WriteLine($"🔄 Processing batch of {batch.Count} messages...");

            // Simulate batch processing
            await Task.Delay(1000);

            foreach (var message in batch)
            {
                Console.WriteLine($"  - Processed: {message.Message.Key} | Partition: {message.Partition} | Offset: {message.Offset}");
            }

            Console.WriteLine($"✅ Batch processing completed!");
        }
    }
}
