using KafkaProducer.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaProducer.Interfaces
{
    public interface IRandomGenerator
    {
        OrderEvent GenerateRandomOrder();
    }
}
