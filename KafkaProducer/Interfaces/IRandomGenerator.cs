using KafkaProducer.Models;

namespace KafkaProducer.Interfaces
{
    public interface IRandomGenerator
    {
        OrderEvent GenerateRandomOrder();
    }
}
