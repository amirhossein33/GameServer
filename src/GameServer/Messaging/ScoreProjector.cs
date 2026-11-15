using GameServer.Data;
using Microsoft.EntityFrameworkCore;

namespace GameServer.Messaging;

/// <summary>
/// Inbox side. Applies a result to the scoreboard exactly once per message id:
/// the inbox row and the score changes are saved in one transaction, and the
/// inbox PK rejects duplicates even if two deliveries race. Score rows carry a
/// version token, so two nodes touching the same player conflict instead of losing an update
/// (the caller retries with a fresh DbContext).
/// </summary>
public sealed class ScoreProjector(AppDbContext db)
{
    public const int WinPoints = 3;
    public const int DrawPoints = 1;

    /// <returns>true if applied, false if the message was a duplicate.</returns>
    public async Task<bool> ProcessAsync(Guid messageId, GameFinishedEvent ev, CancellationToken ct = default)
    {
        if (await db.InboxMessages.AnyAsync(i => i.MessageId == messageId, ct)) return false;

        db.InboxMessages.Add(new InboxMessage { MessageId = messageId, ProcessedAtUtc = DateTime.UtcNow });

        var x = await GetOrAddAsync(ev.PlayerXId, ct);
        var o = await GetOrAddAsync(ev.PlayerOId, ct);

        if (ev.WinnerId is null)
        {
            x.Draws++; x.Points += DrawPoints;
            o.Draws++; o.Points += DrawPoints;
        }
        else
        {
            var (winner, loser) = ev.WinnerId == ev.PlayerXId ? (x, o) : (o, x);
            winner.Wins++; winner.Points += WinPoints;
            loser.Losses++;
        }
        x.Version++;
        o.Version++;

        try
        {
            await db.SaveChangesAsync(ct);
            return true;
        }
        catch (DbUpdateException)
        {
            // Either a concurrent delivery of the same message already won, or we hit a version/PK conflict.
            db.ChangeTracker.Clear();
            if (await db.InboxMessages.AnyAsync(i => i.MessageId == messageId, ct)) return false;
            throw; // conflict: caller retries with a fresh context
        }
    }

    private async Task<PlayerScore> GetOrAddAsync(string playerId, CancellationToken ct)
    {
        var score = await db.PlayerScores.FindAsync([playerId], ct);
        if (score is not null) return score;
        score = new PlayerScore { PlayerId = playerId };
        db.PlayerScores.Add(score);
        return score;
    }
}
