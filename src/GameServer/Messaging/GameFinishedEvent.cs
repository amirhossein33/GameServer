namespace GameServer.Messaging;

public sealed record GameFinishedEvent(
    Guid EventId, string RoomId, string PlayerXId, string PlayerOId,
    string? WinnerId, string Reason, DateTime FinishedAtUtc);
