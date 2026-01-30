using System.Diagnostics;
using System.Text;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using WorkerConsumer.Tracing;

namespace WorkerConsumer;

public class Worker(ILogger<Worker> logger,
    IConfiguration configuration) : BackgroundService
{
    public static readonly string ActivitySourceName = "WorkerConsumer.RabbitMQ";
    private static readonly ActivitySource ActivitySource = new(ActivitySourceName);
    private static readonly TextMapPropagator Propagator = new TraceContextPropagator();

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
        // Extrai o PropagationContext de forma a identificar os message headers
        var parentContext = Propagator.Extract(default, e.BasicProperties, this.ExtractTraceContextFromBasicProperties);
        Baggage.Current = parentContext.Baggage;

        // Semantic convention - OpenTelemetry messaging specification:
        // https://opentelemetry.io/docs/specs/semconv/messaging/rabbitmq/
        var activityName = $"{_exchangeName}:{_queueName} receive";

        using var activity = OpenTelemetryExtensions.ActivitySource
            .StartActivity(activityName, ActivityKind.Consumer, parentContext.ActivityContext);

        var messageContent = Encoding.UTF8.GetString(e.Body.ToArray());
        logger.LogInformation(
            $"[{_queueName} ({e.Exchange}) | Nova mensagem] " + messageContent);

        activity?.SetTag("messaging.system", "rabbitmq");
        activity?.SetTag("messaging.operation", "ack");
        activity?.SetTag("messaging.destination.name", $"{_exchangeName}:{_queueName}");
        activity?.SetTag("messaging.operation.type", "receive");
        activity?.SetTag("messaging.message.id", e.BasicProperties?.MessageId ?? "unknown");
        activity?.SetTag("body", messageContent);        

        // Simular processamento
        await Task.CompletedTask;

        if (activity is not null)
            activity.SetStatus(ActivityStatusCode.Ok);
    }

    private IEnumerable<string> ExtractTraceContextFromBasicProperties(IReadOnlyBasicProperties props, string key)
    {
        try
        {
            if (props.Headers is not null && props.Headers.TryGetValue(key, out var value))
            {
                var bytes = value as byte[];
                return new[] { Encoding.UTF8.GetString(bytes!) };
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, $"Falha durante a extração do trace context: {ex.Message}");
        }

        return Enumerable.Empty<string>();
    }
}