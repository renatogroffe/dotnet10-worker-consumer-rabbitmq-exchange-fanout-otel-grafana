using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace WorkerConsumer
{
    public class Worker(ILogger<Worker> logger,
        IConfiguration configuration) : BackgroundService
    {
        private readonly string _queueName = configuration["RabbitMQ:Queue"]!;
        private readonly string _exchangeName = configuration["RabbitMQ:Exchange"]!;
        private readonly int _intervaloMensagemWorkerAtivo =
            Convert.ToInt32(configuration["IntervaloMensagemWorkerAtivo"]);

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            logger.LogInformation($"Queue = {_queueName}");
            logger.LogInformation("Aguardando mensagens...");

            var factory = new ConnectionFactory()
            {
                Uri = new Uri(configuration.GetConnectionString("RabbitMQ")!)
            };
            using var connection = await factory.CreateConnectionAsync();
            using var channel = await connection.CreateChannelAsync();

            await channel.ExchangeDeclareAsync(
                exchange: _exchangeName,
                type: ExchangeType.Fanout,
                durable: true,
                autoDelete: false,
                arguments: null);
            await channel.QueueDeclareAsync(queue: _queueName,
                                durable: true,
                                exclusive: false,
                                autoDelete: false,
                                arguments: null);
            await channel.QueueBindAsync(
                queue: _queueName,
                exchange: _exchangeName,
                routingKey: string.Empty,
                arguments: null);
            
            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += ProcessarResultado;
            await channel.BasicConsumeAsync(queue: _queueName,
                autoAck: true,
                consumer: consumer);

            while (!stoppingToken.IsCancellationRequested)
            {
                if (logger.IsEnabled(LogLevel.Information))
                {
                    logger.LogInformation(
                        $"Worker ativo em: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
                }
                await Task.Delay(_intervaloMensagemWorkerAtivo, stoppingToken);
            }
        }

        private async Task ProcessarResultado(
            object? sender, BasicDeliverEventArgs e)
        {
            var messageContent = Encoding.UTF8.GetString(e.Body.ToArray());
            logger.LogInformation(
                $"[{_queueName} ({e.Exchange}) | Nova mensagem] " + messageContent);
        }
    }
}