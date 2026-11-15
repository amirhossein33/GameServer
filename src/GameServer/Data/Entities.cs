using System.ComponentModel.DataAnnotations;

namespace GameServer.Data;

public sealed class GameResultEntity
{
    public Guid Id { get; set; }
    public string RoomId { get; set; } = "";
    public string PlayerXId { get; set; } = "";
    public string PlayerOId { get; set; } = "";
    public string? WinnerId { get; set; }
    public string Reason { get; set; } = "";
    public DateTime FinishedAtUtc { get; set; }
}

/// <summary>Written in the same transaction as the business data; published later by OutboxPublisher.</summary>
public sealed class OutboxMessage
{
    public Guid Id { get; set; }
    public string Type { get; set; } = "";
    public string Payload { get; set; } = "";
    public DateTime CreatedAtUtc { get; set; }
    public DateTime? ProcessedAtUtc { get; set; }

    // Retry bookkeeping: failed publishes back off exponentially but are never dropped.
    public int Attempts { get; set; }
    public DateTime? NextAttemptAtUtc { get; set; }
    public string? LastError { get; set; }
}

/// <summary>Written in the same transaction as the score update; the PK makes processing idempotent.</summary>
public sealed class InboxMessage
{
    public Guid MessageId { get; set; }
    public DateTime ProcessedAtUtc { get; set; }
}

public sealed class PlayerScore
{
    public string PlayerId { get; set; } = "";
    public int Points { get; set; }
    public int Wins { get; set; }
    public int Draws { get; set; }
    public int Losses { get; set; }

    /// <summary>Optimistic concurrency token: two nodes updating the same player can't silently lose an update.</summary>
    [ConcurrencyCheck]
    public int Version { get; set; }
}
