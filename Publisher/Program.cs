using System;
using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using System.Threading.Tasks;

class Program
{
    static async Task Main()
    {
        var factory = new ConnectionFactory() { HostName = "localhost", UserName = "guest", Password = "guest" };
        using var connection = await factory.CreateConnectionAsync();
        IChannel channel = await connection.CreateChannelAsync();

        await channel.ExchangeDeclareAsync(exchange: "order_notifications", type: "topic", durable: true);

        Console.WriteLine("Digite ENTER para criar eventos de pedidos...");

        var rnd = new Random();

        while (true)
        {
            Console.ReadLine();

            string orderId = Guid.NewGuid().ToString("N").Substring(0, 8);
            string userId = "user-" + (rnd.Next(1, 4)); 

            var statuses = new[] { "Recebido", "Preparacao", "Dispachado", "Entregue" };

            foreach (var status in statuses)
            {
                var msg = new
                {
                    orderId,
                    userId,
                    status,
                    timestamp = DateTime.UtcNow
                };

                string routingKey = $"user.{userId}";
                var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(msg));


                await channel.BasicPublishAsync(
                    exchange: "order_notifications",
                    routingKey: routingKey,
                    body: body
                );

                Console.WriteLine($"[x] Enviado {status} -> {routingKey} : {JsonSerializer.Serialize(msg)}");
                await Task.Delay(2000);
            }
            Console.WriteLine("---- Pedido simulado enviado para vários status ----");
        }
    }
}
