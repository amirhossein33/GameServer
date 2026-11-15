using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace GameServer.Messaging;

/// <summary>
/// Consumes game results with manual acks; ack only after the inbox+score transaction committed.
/// Retry: transient failures (DB hiccup, optimistic-concurrency conflict) are retried in-process with
/// exponential backoff. After the last attempt the message is rejected WITHOUT requeue, so RabbitMQ
/// routes it to the dead-letter queue instead of looping forever.
/// </summary>
public sealed class ScoreConsumer(
    IServiceScopeFactory scopes, RabbitMqConnection rabbit, ILogger<ScoreConsumer> log) : BackgroundService
{
    private const int MaxAttempts = 4;

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var connection = await rabbit.GetAsync(ct);
                await using var channel = await connection.CreateChannelAsync(cancellationToken: ct);
                await channel.BasicQosAsync(0, 10, false, ct);

                var consumer = new AsyncEventingBasicConsumer(channel);
                consumer.ReceivedAsync += (_, ea) => HandleAsync(channel, ea, ct);
                await channel.BasicConsumeAsync(rabbit.Options.Queue, autoAck: false, consumer: consumer, cancellationToken: ct);

                while (!ct.IsCancellationRequested && channel.IsOpen)
                    await Task.Delay(TimeSpan.FromSeconds(1), ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
            catch (Exception ex)
            {
                log.LogWarning(ex, "Score consumer failed, reconnecting in 3s");
                try { await Task.Delay(TimeSpan.FromSeconds(3), ct); } catch (OperationCanceledException) { }
            }
        }
    }

    private async Task HandleAsync(IChannel channel, BasicDeliverEventArgs ea, CancellationToken ct)
    {
        try
        {
            if (!Guid.TryParse(ea.BasicProperties.MessageId, out var messageId))
            {
                log.LogError("Message without valid MessageId, dead-lettering");
                await channel.BasicRejectAsync(ea.DeliveryTag, requeue: false, cancellationToken: ct);
                return;
            }

            GameFinishedEvent? ev;
            try { ev = JsonSerializer.Deserialize<GameFinishedEvent>(ea.Body.Span, JsonDefaults.Options); }
            catch (JsonException ex)
            {
                log.LogError(ex, "Malformed message {Id}, dead-lettering", messageId);
                await channel.BasicRejectAsync(ea.DeliveryTag, requeue: false, cancellationToken: ct);
                return;
            }
            if (ev is null)
            {
                await channel.BasicRejectAsync(ea.DeliveryTag, requeue: false, cancellationToken: ct);
                return;
            }

            var applied = false;
            await Retry.ExecuteAsync(
                async t =>
                {
                    // Fresh scope (fresh DbContext) per attempt: a failed SaveChanges leaves the context dirty.
                    await using var scope = scopes.CreateAsyncScope();
                    applied = await scope.ServiceProvider.GetRequiredService<ScoreProjector>()
                        .ProcessAsync(messageId, ev, t);
                },
                MaxAttempts, TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(3),
                onRetry: (ex, attempt, wait) =>
                    log.LogWarning(ex, "Processing {Id} failed (attempt {Attempt}/{Max}), retrying in {Wait}",
                        messageId, attempt, MaxAttempts, wait),
                ct: ct);

            if (!applied) log.LogInformation("Duplicate message {Id} ignored", messageId);
            await channel.BasicAckAsync(ea.DeliveryTag, multiple: false, cancellationToken: ct);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            log.LogError(ex, "Giving up on a message after {Max} attempts, dead-lettering", MaxAttempts);
            await channel.BasicRejectAsync(ea.DeliveryTag, requeue: false, cancellationToken: ct);
        }
    }
}
