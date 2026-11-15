using GameServer.Hub;

namespace GameServer.State;

/// <summary>
/// Every node runs this. Expired reconnect deadlines are claimed atomically in Redis,
/// so exactly one node performs the forfeit, even if the node that saw the disconnect has died.
/// </summary>
public sealed class GraceSweeper(RedisGameState state, GameHub hub, ILogger<GraceSweeper> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                foreach (var playerId in await state.ClaimExpiredGraceAsync(50))
                {
                    try { await hub.ExpireAsync(playerId); }
                    catch (Exception ex)
                    {
                        log.LogError(ex, "Expiring {Player} failed, rescheduling", playerId);
                        await state.ScheduleGraceAsync(playerId, TimeSpan.FromSeconds(2));
                    }
                }
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                log.LogWarning(ex, "Grace sweep failed");
            }

            try { await Task.Delay(TimeSpan.FromMilliseconds(500), ct); }
            catch (OperationCanceledException) { }
        }
    }
}
