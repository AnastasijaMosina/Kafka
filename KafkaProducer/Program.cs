using Confluent.Kafka;
using System;
using System.Threading.Tasks;
using KafkaProducer.Producers;

namespace KafkaProducer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Choose producer type:");
            Console.WriteLine("1. Simple Producer");
            Console.WriteLine("2. Advanced Producer");
            var choice = Console.ReadLine();

            if (choice == "2")
            {
                Console.WriteLine("Advanced producer is not implemented yet.");
                // TODO: Call AdvancedProducer.RunAsync() when implemented
            }
            else if (choice == "1")
            {
                await SimpleProducer.RunAsync();
            }
            else
            {
                Console.WriteLine("Invalid choice. Exiting.");
            }
        }
    }
}
