using System.Text;
using GameServer.Data;
using GameServer.State;
using Microsoft.EntityFrameworkCore;
using RabbitMQ.Client;

namespace GameServer.Messaging;

/// <summary>
/// Polls the outbox and publishes with publisher confirms. A row is marked processed only AFTER
/// the broker confirmed it => at-least-once delivery (duplicates are removed by the consumer's inbox).
///
/// Retry: a message that fails to publish gets Attempts++ and an exponential NextAttemptAtUtc
/// (1s, 2s, 4s ... capped at 1 min). It is never dropped; the outbox is the source of truth.
/// With several nodes, a short Redis lock makes one node publish at a time.
/// </summary>
public sealed class OutboxPublisher(
    IServiceScopeFactory scopes, RabbitMqConnection rabbit, RedisGameState redis, ILogger<OutboxPublisher> log)
    : BackgroundService
{
    private const int BatchSize = 50;
    private static readonly TimeSpan RetryBase = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan RetryMax = TimeSpan.FromMinutes(1);

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var connection = await rabbit.GetAsync(ct);
                await using var channel = await connection.CreateChannelAsync(
                    new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true),
                    ct);

                while (!ct.IsCancellationRequested)
                {
                    var published = 0;
                    await using (var handle = await redis.AcquireLockAsync("outbox-publisher", TimeSpan.FromSeconds(30), TimeSpan.Zero))
                    {
                        if (handle is not null) published = await PublishBatchAsync(channel, ct);
                    }
                    if (published == 0) await Task.Delay(TimeSpan.FromMilliseconds(500), ct);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
            catch (Exception ex)
            {
                log.LogWarning(ex, "Outbox publisher failed, reconnecting in 3s");
                try { await Task.Delay(TimeSpan.FromSeconds(3), ct); } catch (OperationCanceledException) { }
            }
        }
    }

    private async Task<int> PublishBatchAsync(IChannel channel, CancellationToken ct)
    {
        await using var scope = scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
        var now = DateTime.UtcNow;

        var batch = await db.OutboxMessages
            .Where(m => m.ProcessedAtUtc == null && (m.NextAttemptAtUtc == null || m.NextAttemptAtUtc <= now))
            .OrderBy(m => m.CreatedAtUtc)
            .Take(BatchSize)
            .ToListAsync(ct);

        foreach (var message in batch)
        {
            try
            {
                var props = new BasicProperties
                {
                    MessageId = message.Id.ToString(),
                    Type = message.Type,
                    ContentType = "application/json",
                    DeliveryMode = DeliveryModes.Persistent
                };

                // With confirmation tracking this awaits the broker ack and throws on nack.
                await channel.BasicPublishAsync(
                    rabbit.Options.Exchange, rabbit.Options.RoutingKey, mandatory: false,
                    basicProperties: props, body: Encoding.UTF8.GetBytes(message.Payload), cancellationToken: ct);

                message.ProcessedAtUtc = DateTime.UtcNow;
                message.LastError = null;
                await db.SaveChangesAsync(ct); // mark right after the confirm
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                message.ProcessedAtUtc = null;
                message.Attempts++;
                message.LastError = ex.Message.Length > 500 ? ex.Message[..500] : ex.Message;
                message.NextAttemptAtUtc = DateTime.UtcNow + Retry.Backoff(message.Attempts, RetryBase, RetryMax);
                await db.SaveChangesAsync(ct);

                log.LogWarning(ex, "Publishing outbox message {Id} failed (attempt {Attempts})", message.Id, message.Attempts);
                if (!channel.IsOpen) throw; // channel is dead: go back to the reconnect loop
            }
        }
        return batch.Count;
    }
}
