using KafkaProducer.Interfaces;
using KafkaProducer.Models;
using System;

namespace KafkaProducer.Utils
{
    internal class RandomGenerator : IRandomGenerator
    {
        private readonly Random _random = new Random();

        public OrderEvent GenerateRandomOrder()
        {
            var products = new[] { "Laptop", "Mouse", "Keyboard", "Monitor", "Headphones" };
            var statuses = new[] { "PENDING", "PROCESSING", "SHIPPED", "DELIVERED" };

            return new OrderEvent
            {
                OrderId = _random.Next(1000, 9999),
                CustomerId = $"CUST-{_random.Next(100, 999)}",
                Amount = Math.Round((decimal)(_random.NextDouble() * 1000 + 50), 2),
                Status = statuses[_random.Next(statuses.Length)],
                Timestamp = DateTime.UtcNow,
                ProductName = products[_random.Next(products.Length)],
                Quantity = _random.Next(1, 5)
            };
        }
    }
}
