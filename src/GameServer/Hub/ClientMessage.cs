namespace GameServer.Hub;

/// <summary>Everything a client may send. The client only states intent; the server decides outcomes.</summary>
public sealed record ClientMessage(string? Type, string? RoomId, int? Cell);
