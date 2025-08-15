using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaConnector.Consumers
{
    internal class OrderProcessingConsumer
    {
        private readonly Dictionary<string, object> _config;

        public OrderProcessingConsumer()
        {
            _config = new Dictionary<string, object>
            {
                ["GroupId"] = "order-processing-group",
                ["BootstrapServers"] = "localhost:9092",
                ["AutoOffsetReset"] = "Earliest",
                ["EnableAutoCommit"] = false,
                ["MaxPollIntervalMs"] = 300000
            };
        }

        public async Task StartProcessing()
        {
            Console.WriteLine("Order Processing Consumer listening to 'quickstart-events' topic on localhost:9092");
            Console.WriteLine("Press Ctrl+C to exit");
            Console.WriteLine();

            Console.WriteLine("=== ORDER PROCESSING CONSUMER CONFIGURATION ===");
            foreach (var config in _config)
            {
                Console.WriteLine($"{config.Key}: {config.Value}");
            }
            Console.WriteLine();

            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true;
                cts.Cancel();
            };

            try
            {
                var orderCount = 0;
                Console.WriteLine("Order processing consumer started. Waiting for orders... (Simulated)");

                while (!cts.Token.IsCancellationRequested)
                {
                    try
                    {
                        // Simulate receiving orders every 4 seconds
                        await Task.Delay(4000, cts.Token);

                        orderCount++;
                        var success = await ProcessOrder(orderCount);

                        if (success)
                        {
                            Console.WriteLine($"[Order] Committed offset {orderCount * 20}");
                        }
                        else
                        {
                            Console.WriteLine($"[Order] Failed to process, message will be retried");
                        }
                        Console.WriteLine();
                    }
                    catch (TaskCanceledException)
                    {
                        break;
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"Processing error: {e.Message}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Shutting down order processing consumer...");
            }
            finally
            {
                Console.WriteLine("Order processing consumer closed.");
            }
        }

        private async Task<bool> ProcessOrder(int orderId)
        {
            try
            {
                var partition = orderId % 2;
                var offset = orderId * 20;
                var orderData = $"{{\"orderId\":{orderId},\"product\":\"Sample Product\",\"quantity\":1,\"price\":99.99}}";

                Console.WriteLine($"[Order] Processing message from partition {partition}, offset {offset}");
                Console.WriteLine($"[Order] Key: order-{orderId}");
                Console.WriteLine($"[Order] Value: {orderData}");

                // Simulate order processing logic
                await SimulateOrderProcessing(orderData);

                Console.WriteLine($"[Order] Successfully processed order {orderId}");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Order] Failed to process order: {ex.Message}");
                return false;
            }
        }

        private async Task SimulateOrderProcessing(string orderData)
        {
            // Simulate various order processing steps
            Console.WriteLine("[Order] Step 1: Validating order...");
            await Task.Delay(50);

            Console.WriteLine("[Order] Step 2: Checking inventory...");
            await Task.Delay(100);

            Console.WriteLine("[Order] Step 3: Processing payment...");
            await Task.Delay(75);

            Console.WriteLine("[Order] Step 4: Updating database...");
            await Task.Delay(25);

            Console.WriteLine("[Order] Step 5: Sending confirmation...");
            await Task.Delay(50);
        }
    }
}

