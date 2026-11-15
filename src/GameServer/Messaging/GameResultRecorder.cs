using System.Text.Json;
using GameServer.Data;
using Microsoft.EntityFrameworkCore;

namespace GameServer.Messaging;

/// <summary>
/// Outbox write side. The result row and the outbox row are added to the same
/// DbContext and saved with ONE SaveChanges => one DB transaction. Either both
/// exist or neither does. Idempotent, so it is safe to retry.
/// </summary>
public sealed class GameResultRecorder(AppDbContext db)
{
    public async Task RecordAsync(GameFinishedEvent ev, CancellationToken ct = default)
    {
        // A previous attempt may have committed even though the caller saw an error.
        if (await db.OutboxMessages.AnyAsync(m => m.Id == ev.EventId, ct)) return;

        db.GameResults.Add(new GameResultEntity
        {
            Id = ev.EventId, RoomId = ev.RoomId, PlayerXId = ev.PlayerXId, PlayerOId = ev.PlayerOId,
            WinnerId = ev.WinnerId, Reason = ev.Reason, FinishedAtUtc = ev.FinishedAtUtc
        });
        db.OutboxMessages.Add(new OutboxMessage
        {
            Id = ev.EventId,
            Type = nameof(GameFinishedEvent),
            Payload = JsonSerializer.Serialize(ev, JsonDefaults.Options),
            CreatedAtUtc = ev.FinishedAtUtc
        });
        await db.SaveChangesAsync(ct);
    }
}
