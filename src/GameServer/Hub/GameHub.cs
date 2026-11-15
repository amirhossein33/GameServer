using System.Net.WebSockets;
using System.Text.Json;
using GameServer.Domain;
using GameServer.Messaging;
using GameServer.State;
using Microsoft.Extensions.Options;

namespace GameServer.Hub;

/// <summary>
/// Stateless w.r.t. games: rooms, membership, presence and timers live in Redis, so any node can serve any
/// player. A node only keeps the sockets that are connected to it. Messages to a player are published on a
/// Redis channel; the node that owns the socket delivers them.
/// </summary>
public sealed class GameHub(
    RedisGameState state, IServiceScopeFactory scopes, TimeProvider time,
    IOptions<GameOptions> options, ILogger<GameHub> log)
{
    private const int MaxMessageBytes = 4096;
    private static readonly TimeSpan LockTtl = TimeSpan.FromSeconds(20);
    private static readonly TimeSpan LockWait = TimeSpan.FromSeconds(3);

    private TimeSpan Grace => options.Value.ReconnectGrace;

    // ───────────────────────── connection lifecycle ─────────────────────────

    public async Task HandleConnectionAsync(string playerId, WebSocket socket, CancellationToken ct)
    {
        var connection = new PlayerConnection(socket);

        // Subscribe first so nothing published for this player is missed.
        var queue = await state.SubscribeAsync(playerId);
        queue.OnMessage(async message => await OnBackplaneMessageAsync(connection, message.Message));

        try
        {
            // A newer socket for the same player (on ANY node) replaces the old one.
            var previous = await state.SwapConnectionAsync(playerId, connection.Token);
            if (previous is not null)
                await state.PublishAsync(playerId, new Envelope("replace", connection.Token, null));
            await state.CancelGraceAsync(playerId);

            await connection.SendAsync(new { type = "connected", playerId, node = Environment.MachineName });
            await ResumeIfInRoomAsync(playerId, connection);

            await ReceiveLoopAsync(playerId, connection, ct);
        }
        finally
        {
            try { await queue.UnsubscribeAsync(); } catch { /* shutting down */ }
            await OnDisconnectedAsync(playerId, connection);
        }
    }

    private async Task ResumeIfInRoomAsync(string playerId, PlayerConnection connection)
    {
        var roomId = await state.GetPlayerRoomAsync(playerId);
        if (roomId is null) return;

        var room = await state.LoadRoomAsync(roomId);
        if (room is null) { await state.ClearPlayerRoomAsync(playerId); return; } // stale membership

        await connection.SendAsync(new { type = "reconnected", room = room.Snapshot() });
        await PublishToAsync(room.PlayerIds().Where(id => id != playerId), new { type = "opponent_reconnected" });
    }

    private static async Task OnBackplaneMessageAsync(PlayerConnection connection, StackExchange.Redis.RedisValue value)
    {
        Envelope? envelope;
        try { envelope = JsonSerializer.Deserialize<Envelope>((string)value!, JsonDefaults.Options); }
        catch (JsonException) { return; }
        if (envelope is null) return;

        if (envelope.Kind == "msg" && envelope.Body is not null)
            await connection.SendRawAsync(envelope.Body);
        else if (envelope.Kind == "replace" && envelope.Token != connection.Token)
            await connection.CloseAsync(WebSocketCloseStatus.PolicyViolation, "replaced by a newer connection");
    }

    private async Task ReceiveLoopAsync(string playerId, PlayerConnection connection, CancellationToken ct)
    {
        var socket = connection.Socket;
        var buffer = new byte[1024];

        try
        {
            while (socket.State == WebSocketState.Open && !ct.IsCancellationRequested)
            {
                using var message = new MemoryStream();
                WebSocketReceiveResult result;
                do
                {
                    result = await socket.ReceiveAsync(new ArraySegment<byte>(buffer), ct);
                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        await connection.CloseAsync(WebSocketCloseStatus.NormalClosure, "bye");
                        return;
                    }
                    message.Write(buffer, 0, result.Count);
                    if (message.Length > MaxMessageBytes)
                    {
                        await connection.CloseAsync(WebSocketCloseStatus.MessageTooBig, "message too large");
                        return;
                    }
                } while (!result.EndOfMessage);

                if (result.MessageType != WebSocketMessageType.Text)
                {
                    await SendErrorAsync(connection, "text_only");
                    continue;
                }

                ClientMessage? msg;
                try { msg = JsonSerializer.Deserialize<ClientMessage>(message.ToArray(), JsonDefaults.Options); }
                catch (JsonException) { msg = null; }

                if (msg?.Type is null) { await SendErrorAsync(connection, "invalid_message"); continue; }

                try { await DispatchAsync(playerId, connection, msg); }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    log.LogError(ex, "Handling {Type} from {Player} failed", msg.Type, playerId);
                    await SendErrorAsync(connection, "internal_error");
                }
            }
        }
        catch (WebSocketException) { /* abrupt disconnect */ }
        catch (OperationCanceledException) { /* server shutting down */ }
    }

    private async Task OnDisconnectedAsync(string playerId, PlayerConnection connection)
    {
        try
        {
            // If another socket already replaced this one, this is not a real disconnect.
            if (!await state.ClearConnectionIfCurrentAsync(playerId, connection.Token)) return;

            var roomId = await state.GetPlayerRoomAsync(playerId);
            if (roomId is null) return;
            var room = await state.LoadRoomAsync(roomId);
            if (room is null) return;

            await PublishToAsync(room.PlayerIds().Where(id => id != playerId),
                new { type = "opponent_disconnected", graceSeconds = (int)Grace.TotalSeconds });

            // The deadline lives in Redis; GraceSweeper (on any node) enforces it.
            await state.ScheduleGraceAsync(playerId, Grace);
        }
        catch (Exception ex)
        {
            log.LogError(ex, "Disconnect handling for {Player} failed", playerId);
        }
    }

    /// <summary>Called by GraceSweeper after it atomically claimed an expired deadline.</summary>
    public async Task ExpireAsync(string playerId)
    {
        if (await state.IsConnectedAsync(playerId)) return; // came back at the last moment

        var roomId = await state.GetPlayerRoomAsync(playerId);
        if (roomId is null) return;

        RoomSnapshot? finalSnapshot = null;
        await using (var handle = await state.AcquireLockAsync($"room:{roomId}", LockTtl, LockWait))
        {
            if (handle is null)
            {
                await state.ScheduleGraceAsync(playerId, TimeSpan.FromSeconds(1)); // try again shortly
                return;
            }

            var room = await state.LoadRoomAsync(roomId);
            if (room is null) { await state.ClearPlayerRoomAsync(playerId); return; }

            switch (room.Abandon(playerId))
            {
                case AbandonResult.Finished:
                    finalSnapshot = await CompleteGameAsync(room);
                    break;
                case AbandonResult.Cancelled:
                    await state.DeleteRoomAsync(room.Id, room.PlayerIds());
                    break;
            }
        }

        if (finalSnapshot is not null)
            await BroadcastAsync(finalSnapshot, new { type = "game_over", room = finalSnapshot });
    }

    // ───────────────────────── message handling ─────────────────────────

    private Task DispatchAsync(string playerId, PlayerConnection connection, ClientMessage msg) => msg.Type switch
    {
        "create_room" => CreateRoomAsync(playerId, connection),
        "join_room" => JoinRoomAsync(playerId, connection, msg.RoomId),
        "move" => MoveAsync(playerId, connection, msg.Cell),
        _ => SendErrorAsync(connection, "unknown_message_type")
    };

    private async Task CreateRoomAsync(string playerId, PlayerConnection connection)
    {
        GameRoom room;
        do { room = new GameRoom(Guid.NewGuid().ToString("N")[..6].ToUpperInvariant(), playerId); }
        while (!await state.TryCreateRoomAsync(room));

        if (!await ReserveMembershipAsync(playerId, room.Id))
        {
            await state.DeleteRoomAsync(room.Id, []);
            await SendErrorAsync(connection, "already_in_room");
            return;
        }
        await connection.SendAsync(new { type = "room_created", room = room.Snapshot() });
    }

    private async Task JoinRoomAsync(string playerId, PlayerConnection connection, string? requestedRoomId)
    {
        if (string.IsNullOrWhiteSpace(requestedRoomId)) { await SendErrorAsync(connection, "room_not_found"); return; }
        var roomId = requestedRoomId.Trim().ToUpperInvariant();

        if (!await ReserveMembershipAsync(playerId, roomId))
        {
            await SendErrorAsync(connection, "already_in_room");
            return;
        }

        RoomSnapshot? snapshot = null;
        string? error = null;

        await using (var handle = await state.AcquireLockAsync($"room:{roomId}", LockTtl, LockWait))
        {
            if (handle is null) error = "room_busy";
            else
            {
                var room = await state.LoadRoomAsync(roomId);
                if (room is null) error = "room_not_found";
                else
                {
                    var joinError = room.Join(playerId);
                    if (joinError != JoinError.None) error = ToCode(joinError);
                    else
                    {
                        await state.SaveRoomAsync(room);
                        snapshot = room.Snapshot();
                    }
                }
            }
        }

        if (error is not null || snapshot is null)
        {
            await state.ClearPlayerRoomAsync(playerId); // roll back the reservation
            await SendErrorAsync(connection, error ?? "internal_error");
            return;
        }
        await BroadcastAsync(snapshot, new { type = "game_started", room = snapshot });
    }

    private async Task MoveAsync(string playerId, PlayerConnection connection, int? cell)
    {
        if (cell is null) { await SendErrorAsync(connection, "cell_required"); return; }

        var roomId = await state.GetPlayerRoomAsync(playerId);
        if (roomId is null) { await SendErrorAsync(connection, "not_in_room"); return; }

        RoomSnapshot? snapshot = null;
        bool finished = false;
        string? error = null;

        await using (var handle = await state.AcquireLockAsync($"room:{roomId}", LockTtl, LockWait))
        {
            if (handle is null) error = "room_busy";
            else
            {
                var room = await state.LoadRoomAsync(roomId);
                if (room is null)
                {
                    await state.ClearPlayerRoomAsync(playerId);
                    error = "not_in_room";
                }
                else
                {
                    var result = room.Move(playerId, cell.Value);
                    if (result.Error != MoveError.None) error = ToCode(result.Error);
                    else if (result.Outcome is null)
                    {
                        await state.SaveRoomAsync(room);
                        snapshot = room.Snapshot();
                    }
                    else
                    {
                        snapshot = await CompleteGameAsync(room);
                        finished = true;
                    }
                }
            }
        }

        if (error is not null || snapshot is null)
        {
            await SendErrorAsync(connection, error ?? "internal_error");
            return;
        }
        await BroadcastAsync(snapshot, new { type = finished ? "game_over" : "state", room = snapshot });
    }

    // ───────────────────────── finishing a game ─────────────────────────

    /// <summary>Runs under the room lock: persist result (outbox) with retry, then drop the room from Redis.</summary>
    private async Task<RoomSnapshot> CompleteGameAsync(GameRoom room)
    {
        var snap = room.Snapshot();
        var ev = new GameFinishedEvent(
            Guid.NewGuid(), snap.RoomId, snap.PlayerX!, snap.PlayerO!,
            snap.WinnerId, snap.Reason!, time.GetUtcNow().UtcDateTime);

        await PersistResultAsync(ev);
        await state.DeleteRoomAsync(room.Id, room.PlayerIds());
        return snap;
    }

    /// <summary>
    /// Outbox write with retry (5 attempts, exponential backoff). RabbitMQ is NOT touched here, so a
    /// broker outage never affects gameplay. If the DB stays down, the event is parked in a Redis list
    /// so the result is not silently lost.
    /// </summary>
    private async Task PersistResultAsync(GameFinishedEvent ev)
    {
        try
        {
            await Retry.ExecuteAsync(
                async ct =>
                {
                    await using var scope = scopes.CreateAsyncScope();
                    await scope.ServiceProvider.GetRequiredService<GameResultRecorder>().RecordAsync(ev, ct);
                },
                maxAttempts: 5,
                baseDelay: TimeSpan.FromMilliseconds(200),
                maxDelay: TimeSpan.FromSeconds(2),
                onRetry: (ex, attempt, wait) =>
                    log.LogWarning(ex, "Persisting result of room {Room} failed (attempt {Attempt}), retrying in {Wait}",
                        ev.RoomId, attempt, wait));
        }
        catch (Exception ex)
        {
            log.LogCritical(ex, "Could not persist result of room {Room}; parking it in Redis", ev.RoomId);
            try { await state.PushDeadResultAsync(JsonSerializer.Serialize(ev, JsonDefaults.Options)); }
            catch (Exception inner) { log.LogCritical(inner, "Could not park result of room {Room}: {Event}", ev.RoomId, ev); }
        }
    }

    // ───────────────────────── helpers ─────────────────────────

    private Task BroadcastAsync(RoomSnapshot snapshot, object message) =>
        PublishToAsync(new[] { snapshot.PlayerX, snapshot.PlayerO }.OfType<string>(), message);

    private Task PublishToAsync(IEnumerable<string> playerIds, object message)
    {
        var envelope = new Envelope("msg", null, JsonSerializer.Serialize(message, JsonDefaults.Options));
        return Task.WhenAll(playerIds.Select(id => state.PublishAsync(id, envelope)));
    }

    /// <summary>Membership is a Redis key (SET NX). A stale key pointing at a missing room is cleaned up.</summary>
    private async Task<bool> ReserveMembershipAsync(string playerId, string roomId)
    {
        if (await state.TryReservePlayerRoomAsync(playerId, roomId)) return true;

        var existing = await state.GetPlayerRoomAsync(playerId);
        if (existing is not null && await state.LoadRoomAsync(existing) is null)
        {
            await state.ClearPlayerRoomAsync(playerId);
            return await state.TryReservePlayerRoomAsync(playerId, roomId);
        }
        return false;
    }

    private static Task SendErrorAsync(PlayerConnection connection, string code) =>
        connection.SendAsync(new { type = "error", code });

    private static string ToCode<TEnum>(TEnum value) where TEnum : struct, Enum =>
        JsonNamingPolicy.SnakeCaseLower.ConvertName(value.ToString());
}
