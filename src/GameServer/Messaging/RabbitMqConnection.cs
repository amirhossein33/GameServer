using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace GameServer.Messaging;

/// <summary>Shared connection + idempotent topology declaration (durable exchange/queue/binding + dead-letter queue).</summary>
public sealed class RabbitMqConnection(IOptions<RabbitMqOptions> options) : IAsyncDisposable
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private IConnection? _connection;

    public RabbitMqOptions Options => options.Value;

    public async Task<IConnection> GetAsync(CancellationToken ct)
    {
        if (_connection is { IsOpen: true }) return _connection;

        await _gate.WaitAsync(ct);
        try
        {
            if (_connection is { IsOpen: true }) return _connection;
            if (_connection is not null) await _connection.DisposeAsync();

            var o = options.Value;
            var factory = new ConnectionFactory
            {
                HostName = o.Host, Port = o.Port, UserName = o.User, Password = o.Password
            };
            _connection = await factory.CreateConnectionAsync(ct);

            await using var ch = await _connection.CreateChannelAsync(cancellationToken: ct);

            // Dead-letter side: messages that exhausted their retries end up here for inspection/replay.
            await ch.ExchangeDeclareAsync(o.DeadLetterExchange, ExchangeType.Direct, durable: true, cancellationToken: ct);
            await ch.QueueDeclareAsync(o.DeadLetterQueue, durable: true, exclusive: false, autoDelete: false, cancellationToken: ct);
            await ch.QueueBindAsync(o.DeadLetterQueue, o.DeadLetterExchange, o.DeadLetterRoutingKey, cancellationToken: ct);

            await ch.ExchangeDeclareAsync(o.Exchange, ExchangeType.Direct, durable: true, cancellationToken: ct);
            var args = new Dictionary<string, object?>
            {
                ["x-dead-letter-exchange"] = o.DeadLetterExchange,
                ["x-dead-letter-routing-key"] = o.DeadLetterRoutingKey
            };
            await ch.QueueDeclareAsync(o.Queue, durable: true, exclusive: false, autoDelete: false, arguments: args, cancellationToken: ct);
            await ch.QueueBindAsync(o.Queue, o.Exchange, o.RoutingKey, cancellationToken: ct);
            return _connection;
        }
        finally { _gate.Release(); }
    }

    public async ValueTask DisposeAsync()
    {
        if (_connection is not null) await _connection.DisposeAsync();
        _gate.Dispose();
    }
}
