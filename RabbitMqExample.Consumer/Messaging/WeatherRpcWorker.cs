using System.Text.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMqExample.Consumer.Services;

namespace RabbitMqExample.Consumer.Messaging;

/// <summary>
/// Escucha solicitudes RPC en la cola "weather" y publica cada respuesta en ReplyTo.
/// </summary>
public sealed class WeatherRpcWorker(
    IOptions<RabbitMqOptions> options,
    WeatherForecastService weatherService,
    ILogger<WeatherRpcWorker> logger
) : BackgroundService
{
    private const string RequestQueue = "weather";
    private readonly RabbitMqOptions _options = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        ConnectionFactory factory = new()
        {
            HostName = _options.HostName,
            Port = _options.Port,
            UserName = _options.UserName,
            Password = _options.Password,
            VirtualHost = _options.VirtualHost,
            AutomaticRecoveryEnabled = true,
        };

        await using IConnection connection = await factory.CreateConnectionAsync(
            "rabbitmq-basic-consumer",
            stoppingToken
        );
        await using IChannel channel = await connection.CreateChannelAsync(
            cancellationToken: stoppingToken
        );

        // Es una cola compartida de trabajo, por eso es duradera, no exclusiva y
        // no se elimina automáticamente. Esta declaración es válida en RabbitMQ 4.3.
        await channel.QueueDeclareAsync(
            queue: RequestQueue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            cancellationToken: stoppingToken
        );

        // Entrega una solicitud cada vez. RabbitMQ no envía la siguiente hasta
        // que este consumidor confirma la actual mediante BasicAckAsync.
        await channel.BasicQosAsync(
            prefetchSize: 0,
            prefetchCount: 1,
            global: false,
            cancellationToken: stoppingToken
        );

        AsyncEventingBasicConsumer consumer = new(channel);
        consumer.ReceivedAsync += (_, eventArgs) =>
            HandleRequestAsync(channel, eventArgs, stoppingToken);

        await channel.BasicConsumeAsync(
            queue: RequestQueue,
            autoAck: false,
            consumer: consumer,
            cancellationToken: stoppingToken
        );

        logger.LogInformation("RPC consumer listening on queue {Queue}.", RequestQueue);

        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            logger.LogInformation("RPC consumer is stopping.");
        }
    }

    private async Task HandleRequestAsync(
        IChannel channel,
        BasicDeliverEventArgs eventArgs,
        CancellationToken cancellationToken
    )
    {
        string? correlationId = eventArgs.BasicProperties.CorrelationId;
        string? replyTo = eventArgs.BasicProperties.ReplyTo;

        try
        {
            if (string.IsNullOrWhiteSpace(correlationId) || string.IsNullOrWhiteSpace(replyTo))
            {
                throw new InvalidOperationException(
                    "The RPC request must include CorrelationId and ReplyTo."
                );
            }

            logger.LogInformation("Processing RPC request {CorrelationId}.", correlationId);

            // Paso 4: ejecuta la lógica de negocio y serializa su resultado.
            WeatherForecast[] forecasts = weatherService.CreateForecasts();
            byte[] responseBody = JsonSerializer.SerializeToUtf8Bytes(forecasts);

            BasicProperties responseProperties = new()
            {
                ContentType = "application/json",
                CorrelationId = correlationId,
            };

            // Paso 5: el exchange predeterminado enruta la respuesta directamente
            // a la cola temporal cuyo nombre llegó en la propiedad ReplyTo.
            await channel.BasicPublishAsync(
                exchange: string.Empty,
                routingKey: replyTo,
                mandatory: true,
                basicProperties: responseProperties,
                body: responseBody,
                cancellationToken: cancellationToken
            );

            // Confirma la solicitud solo después de publicar su respuesta.
            await channel.BasicAckAsync(
                deliveryTag: eventArgs.DeliveryTag,
                multiple: false,
                cancellationToken: cancellationToken
            );

            logger.LogInformation("Published RPC response {CorrelationId}.", correlationId);
        }
        catch (Exception exception)
        {
            logger.LogError(
                exception,
                "Could not process RPC request {CorrelationId}.",
                correlationId
            );

            if (channel.IsOpen)
            {
                // El ejemplo descarta mensajes inválidos para evitar un bucle infinito.
                await channel.BasicNackAsync(
                    deliveryTag: eventArgs.DeliveryTag,
                    multiple: false,
                    requeue: false,
                    cancellationToken: cancellationToken
                );
            }
        }
    }
}
