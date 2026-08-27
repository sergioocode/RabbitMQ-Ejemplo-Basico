using System.Text.Json;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMqExample.Api.Messaging;

/// <summary>
/// Publica solicitudes en RabbitMQ y espera la respuesta correspondiente.
/// </summary>
public sealed class RabbitMqRpcClient(
    IOptions<RabbitMqOptions> options,
    ILogger<RabbitMqRpcClient> logger
) : IHostedService, IAsyncDisposable
{
    private static readonly TimeSpan ResponseTimeout = TimeSpan.FromSeconds(10);
    private readonly RabbitMqOptions _options = options.Value;
    private IConnection? _connection;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        ConnectionFactory factory = CreateConnectionFactory();
        _connection = await factory.CreateConnectionAsync("rabbitmq-basic-api", cancellationToken);
        logger.LogInformation("RPC client connected to RabbitMQ.");
    }

    public async Task<JsonElement> CallAsync(
        string requestQueue,
        CancellationToken cancellationToken
    )
    {
        IConnection connection =
            _connection ?? throw new InvalidOperationException("RabbitMQ connection is not ready.");

        // Cada llamada utiliza su propio canal. Así varias peticiones HTTP pueden
        // ejecutarse en paralelo sin compartir un canal, que no es thread-safe.
        await using IChannel channel = await connection.CreateChannelAsync(
            cancellationToken: cancellationToken
        );

        // La cola de trabajo es compartida y debe sobrevivir al reinicio del broker.
        // Esta combinación es válida en RabbitMQ 4.3: durable=true, exclusive=false.
        await channel.QueueDeclareAsync(
            queue: requestQueue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            cancellationToken: cancellationToken
        );

        // RabbitMQ genera un nombre amq.gen-... para la respuesta. Como pertenece
        // solo a esta llamada, es no duradera, exclusiva y autodestructiva.
        QueueDeclareOk replyQueue = await channel.QueueDeclareAsync(
            queue: string.Empty,
            durable: false,
            exclusive: true,
            autoDelete: true,
            arguments: null,
            cancellationToken: cancellationToken
        );

        string correlationId = Guid.NewGuid().ToString("N");
        TaskCompletionSource<byte[]> responseSource = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );

        AsyncEventingBasicConsumer consumer = new(channel);
        consumer.ReceivedAsync += (_, eventArgs) =>
        {
            // RabbitMQ.Client reutiliza la memoria del cuerpo tras finalizar el
            // callback; ToArray conserva una copia segura para completar el await.
            if (eventArgs.BasicProperties.CorrelationId == correlationId)
            {
                responseSource.TrySetResult(eventArgs.Body.ToArray());
            }

            return Task.CompletedTask;
        };

        await channel.BasicConsumeAsync(
            queue: replyQueue.QueueName,
            autoAck: true,
            consumer: consumer,
            cancellationToken: cancellationToken
        );

        BasicProperties properties = new()
        {
            ContentType = "application/json",
            CorrelationId = correlationId,
            ReplyTo = replyQueue.QueueName,
            Persistent = true,
        };

        byte[] requestBody = JsonSerializer.SerializeToUtf8Bytes(new { });

        logger.LogInformation(
            "Publishing RPC request {CorrelationId} to queue {Queue}.",
            correlationId,
            requestQueue
        );

        await channel.BasicPublishAsync(
            exchange: string.Empty,
            routingKey: requestQueue,
            mandatory: true,
            basicProperties: properties,
            body: requestBody,
            cancellationToken: cancellationToken
        );

        using CancellationTokenSource timeoutSource =
            CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutSource.CancelAfter(ResponseTimeout);

        byte[] responseBody;
        try
        {
            responseBody = await responseSource.Task.WaitAsync(timeoutSource.Token);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException(
                $"RabbitMQ did not answer request {correlationId} within {ResponseTimeout.TotalSeconds} seconds."
            );
        }

        logger.LogInformation("Received RPC response {CorrelationId}.", correlationId);
        return JsonSerializer.Deserialize<JsonElement>(responseBody);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await CloseConnectionAsync(cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        await CloseConnectionAsync(CancellationToken.None);
    }

    private ConnectionFactory CreateConnectionFactory()
    {
        return new()
        {
            HostName = _options.HostName,
            Port = _options.Port,
            UserName = _options.UserName,
            Password = _options.Password,
            VirtualHost = _options.VirtualHost,
            AutomaticRecoveryEnabled = true,
        };
    }

    private async Task CloseConnectionAsync(CancellationToken cancellationToken)
    {
        IConnection? connection = Interlocked.Exchange(ref _connection, null);
        if (connection is null)
        {
            return;
        }

        if (connection.IsOpen)
        {
            await connection.CloseAsync(cancellationToken);
        }

        await connection.DisposeAsync();
    }
}
