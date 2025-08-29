using KafkaProducer.Interfaces;
using KafkaProducer.Producers;
using KafkaProducer.Utils;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);

// Register configuration
builder.Configuration.AddJsonFile("appsettings.json", optional: true);

// Register services
builder.Services.AddSingleton<IRandomGenerator, RandomGenerator>();
builder.Services.AddSingleton<SchemaBasedProducer>();
//SimpleProducer is a static class and cannot be registered as a service
// builder.Services.AddSingleton<SimpleProducer>();

var host = builder.Build();

Console.WriteLine("Choose producer type:");
Console.WriteLine("1. Simple Producer");
Console.WriteLine("2. Advanced Producer");
var choice = Console.ReadLine();

if (choice == "1")
{
    await SimpleProducer.RunAsync(builder.Configuration);
}
else if (choice == "2")
{
    var schemaBasedProducer = host.Services.GetRequiredService<SchemaBasedProducer>();
    await schemaBasedProducer.ProduceOrderEvents();
}
else
{
    Console.WriteLine("Invalid choice. Exiting.");
}
