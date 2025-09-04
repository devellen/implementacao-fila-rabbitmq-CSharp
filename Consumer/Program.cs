using System;
using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

class Program
{
    static async Task Main(string[] args)
    {
        string userId = args.Length > 0 ? args[0] : "user-1";
        Console.WriteLine($"Consumer iniciado para {userId}");

        var factory = new ConnectionFactory() { HostName = "localhost", UserName = "guest", Password = "guest" };
        using var connection = await factory.CreateConnectionAsync();
        IChannel channel = await connection.CreateChannelAsync();

        await channel.ExchangeDeclareAsync(exchange: "order_notifications", type: "topic", durable: true);

        string queueName = await channel.QueueDeclareAsync(
            queue: $"notifications.{userId}", durable: true, exclusive: false, autoDelete: false, arguments: null);

        string routingKey = $"user.{userId}";
        await channel.QueueBindAsync(queue: queueName, exchange: "order_notifications", routingKey: routingKey);

        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (model, ea) =>
        {
            var body = ea.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);

            try
            {
                var doc = JsonSerializer.Deserialize<JsonElement>(message);
                Console.WriteLine($"[Notificado para {userId}] {message}");

                await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro processando mensagem: {ex.Message}");

                await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
            }
        };

        await channel.BasicConsumeAsync(queue: queueName, autoAck: false, consumer: consumer);

        Console.WriteLine("Pressione [enter] para sair.");
        Console.ReadLine();
    }
}
