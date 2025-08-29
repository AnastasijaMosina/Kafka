using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaConnector.Consumers
{
    internal class DeadLetterQueueConsumer
    {

        // TODO: add 3 attempts for 

        private readonly ConsumerConfig _consumerConfig;
        private readonly ProducerConfig _producerConfig;
        private readonly Dictionary<string, int> _retryCount = new Dictionary<string, int>();
        private const int MaxRetries = 3;

        public DeadLetterQueueConsumer()
        {
            _consumerConfig = new ConsumerConfig
            {
                GroupId = "dlq-consumer-group",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
                SessionTimeoutMs = 6000,
                MaxPollIntervalMs = 300000
            };

            _producerConfig = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                Acks = Acks.All,
                MessageSendMaxRetries = 3,
                EnableIdempotence = true
            };
        }

        public async Task StartConsuming(string topicName)
        {
            Console.WriteLine("DLQ Consumer started. Processing order events with error handling...");
            Console.WriteLine("Press Ctrl+C to exit");

            var consumer = new ConsumerBuilder<string, string>(_consumerConfig).Build();
            var dlqProducer = new ProducerBuilder<string, string>(_producerConfig).Build();

            consumer.Subscribe(topicName);

            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true;
                cts.Cancel();
            };

            try
            {
                while (!cts.Token.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(cts.Token);
                        var messageKey = consumeResult.Message.Key ?? "unknown";

                        Console.WriteLine($"Processing message: {messageKey}");

                        try
                        {
                            await ProcessMessage(consumeResult.Message.Value);

                            // Reset retry count on successful processing
                            _retryCount.Remove(messageKey);
                            consumer.Commit(consumeResult);
                            Console.WriteLine($"✓ Successfully processed message: {messageKey}");
                        }
                        catch (Exception processingEx)
                        {
                            await HandleProcessingError(dlqProducer, consumeResult, processingEx, messageKey);
                            consumer.Commit(consumeResult);
                        }
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Consume error: {e.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("DLQ consumer shutting down...");
            }
            finally
            {
                consumer.Close();
                Console.WriteLine("DLQ consumer closed.");
            }
        }

        private async Task ProcessMessage(string messageValue)
        {
            // Simulate processing with random failures
            var random = new Random();
            if (random.Next(1, 5) == 1) // 25% chance of failure
            {
                throw new InvalidOperationException("Simulated processing error");
            }

            // Simulate processing time
            await Task.Delay(500);

            // Parse and validate the order
            var order = JsonConvert.DeserializeObject<dynamic>(messageValue);
            if (order?.OrderId == null)
            {
                throw new ArgumentException("Invalid order format");
            }
        }

        private async Task HandleProcessingError(IProducer<string, string> dlqProducer,
            ConsumeResult<string, string> consumeResult, Exception ex, string messageKey)
        {
            _retryCount.TryGetValue(messageKey, out int currentRetries);
            currentRetries++;
            _retryCount[messageKey] = currentRetries;

            Console.WriteLine($"⚠ Processing failed for {messageKey} (attempt {currentRetries}/{MaxRetries}): {ex.Message}");

            //if (currentRetries >= MaxRetries)
            //{
                // Send to Dead Letter Queue
                var dlqMessage = new Message<string, string>
                {
                    Key = consumeResult.Message.Key,
                    Value = consumeResult.Message.Value,
                    // Replace this line:
                    // Headers = new Headers(consumeResult.Message.Headers)

                    // With the following code to copy headers manually:
                    Headers = CloneHeaders(consumeResult.Message.Headers)
                };

                // Add error metadata
                dlqMessage.Headers.Add("error-reason", System.Text.Encoding.UTF8.GetBytes(ex.Message));
                dlqMessage.Headers.Add("error-timestamp", System.Text.Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("O")));
                dlqMessage.Headers.Add("retry-count", System.Text.Encoding.UTF8.GetBytes(currentRetries.ToString()));
                dlqMessage.Headers.Add("original-topic", System.Text.Encoding.UTF8.GetBytes(consumeResult.Topic));

                try
                {
                    await dlqProducer.ProduceAsync("order-events-dlq", dlqMessage);
                    Console.WriteLine($"💀 Message {messageKey} sent to Dead Letter Queue after {MaxRetries} failed attempts");
                    _retryCount.Remove(messageKey);
                }
                catch (ProduceException<string, string> pe)
                {
                    Console.WriteLine($"Failed to send message to DLQ: {pe.Error.Reason}");
                }
            //}
        }
        private Headers CloneHeaders(Headers originalHeaders)
        {
            var newHeaders = new Headers();
            if (originalHeaders != null)
            {
                foreach (var header in originalHeaders)
                {
                    newHeaders.Add(header.Key, header.GetValueBytes());
                }
            }
            return newHeaders;
        }
    }
}
