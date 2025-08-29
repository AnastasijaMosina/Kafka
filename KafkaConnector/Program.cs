using KafkaConnector.Consumers;
using System;
using System.Threading.Tasks;

namespace KafkaConnector
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Choose consumer type:");
            Console.WriteLine("1. Basic Consumer");
            Console.WriteLine("2. Advanced Consumer");
            Console.WriteLine("3. Order Processing Consumer");
            Console.WriteLine("4. Dead Letter Queue(DLQ) Consumer");

            var choice = Console.ReadLine();

            switch (choice)
            {
                case "1":
                    var basicConsumer = new BasicKafkaConsumer();       
                    basicConsumer.Run();
                    break;
                case "2":
                    await RunAdvancedConsumer();
                    break;
                case "3":
                    await RunOrderProcessor();
                    break;
                default:
                    Console.WriteLine("Invalid choice");
                    break;
            }
        }

        static async Task RunAdvancedConsumer()
        {
            var consumer = new AdvancedKafkaConsumer();
            await consumer.StartConsuming("simple-events");
        }

        static async Task RunOrderProcessor()
        {
            var processor = new OrderProcessingConsumer();
            await processor.StartProcessing();
        }
    }
}
