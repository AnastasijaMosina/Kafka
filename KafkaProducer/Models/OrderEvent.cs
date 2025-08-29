using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaProducer.Models
{
    public class OrderEvent
    {
        public int OrderId { get; set; }
        public string CustomerId { get; set; }
        public decimal Amount { get; set; }
        public string Status { get; set; }
        public DateTime Timestamp { get; set; }
        public string ProductName { get; set; }
        public int Quantity { get; set; }
    }
}
