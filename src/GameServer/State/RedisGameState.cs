using System.Diagnostics;
using System.Text.Json;
using GameServer.Domain;
using StackExchange.Redis;

namespace GameServer.State;

/// <summary>Message sent over the Redis backplane to the node that holds a player's socket.</summary>
public sealed record Envelope(string Kind, string? Token, string? Body);

/// <summary>
/// Everything that must be shared between nodes lives here:
///   room state, player->room membership, distributed locks, connection presence,
///   the reconnect-grace schedule (sorted set) and the pub/sub backplane.
/// </summary>
public sealed class RedisGameState(IConnectionMultiplexer mux, TimeProvider time)
{
    private static readonly TimeSpan KeyTtl = TimeSpan.FromHours(24);
    private const string GraceKey = "game:grace";
    private const string DeadResultsKey = "game:dead-results";

    private const string CompareAndDeleteScript =
        "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";

    private IDatabase Db => mux.GetDatabase();
    private ISubscriber Sub => mux.GetSubscriber();

    private static string RoomKey(string id) => $"game:room:{id}";
    private static string PlayerRoomKey(string id) => $"game:player-room:{id}";
    private static string ConnKey(string id) => $"game:conn:{id}";
    private static string LockKey(string name) => $"game:lock:{name}";
    private static RedisChannel Channel(string playerId) => RedisChannel.Literal($"game:player:{playerId}");

    // ───────────── rooms ─────────────

    public Task<bool> TryCreateRoomAsync(GameRoom room) =>
        Db.StringSetAsync(RoomKey(room.Id), Serialize(room), KeyTtl, when: When.NotExists);

    public Task SaveRoomAsync(GameRoom room) =>
        Db.StringSetAsync(RoomKey(room.Id), Serialize(room), KeyTtl);

    public async Task<GameRoom?> LoadRoomAsync(string roomId)
    {
        var value = await Db.StringGetAsync(RoomKey(roomId));
        if (value.IsNullOrEmpty) return null;
        var state = JsonSerializer.Deserialize<RoomState>((string)value!, JsonDefaults.Options);
        return state is null ? null : GameRoom.FromState(state);
    }

    /// <summary>Removes the room and everything pointing at it (membership, pending grace entries).</summary>
    public async Task DeleteRoomAsync(string roomId, IEnumerable<string> playerIds)
    {
        var tasks = new List<Task> { Db.KeyDeleteAsync(RoomKey(roomId)) };
        foreach (var p in playerIds)
        {
            tasks.Add(Db.KeyDeleteAsync(PlayerRoomKey(p)));
            tasks.Add(Db.SortedSetRemoveAsync(GraceKey, p));
        }
        await Task.WhenAll(tasks);
    }

    // ───────────── membership ─────────────

    public Task<bool> TryReservePlayerRoomAsync(string playerId, string roomId) =>
        Db.StringSetAsync(PlayerRoomKey(playerId), roomId, KeyTtl, when: When.NotExists);

    public async Task<string?> GetPlayerRoomAsync(string playerId)
    {
        var value = await Db.StringGetAsync(PlayerRoomKey(playerId));
        return value.IsNullOrEmpty ? null : (string?)value;
    }

    public Task ClearPlayerRoomAsync(string playerId) => Db.KeyDeleteAsync(PlayerRoomKey(playerId));

    // ───────────── distributed lock ─────────────

    /// <summary>Returns a handle (dispose to release) or null if the lock could not be taken within <paramref name="wait"/>.</summary>
    public async Task<IAsyncDisposable?> AcquireLockAsync(string name, TimeSpan ttl, TimeSpan wait)
    {
        var key = LockKey(name);
        var token = Guid.NewGuid().ToString("N");
        var sw = Stopwatch.StartNew();
        do
        {
            if (await Db.StringSetAsync(key, token, ttl, when: When.NotExists))
                return new LockHandle(this, key, token);
            if (wait > TimeSpan.Zero) await Task.Delay(40);
        } while (sw.Elapsed < wait);
        return null;
    }

    private async Task<bool> CompareAndDeleteAsync(string key, string token)
    {
        var result = await Db.ScriptEvaluateAsync(
            CompareAndDeleteScript, new RedisKey[] { key }, new RedisValue[] { token });
        return (int)result == 1;
    }

    private sealed class LockHandle(RedisGameState owner, string key, string token) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            try { await owner.CompareAndDeleteAsync(key, token); } catch { /* lock expires by TTL anyway */ }
        }
    }

    // ───────────── presence ─────────────

    /// <returns>The previous connection token for this player, if any.</returns>
    public async Task<string?> SwapConnectionAsync(string playerId, string token)
    {
        var previous = await Db.StringSetAndGetAsync(ConnKey(playerId), token, KeyTtl);
        return previous.IsNullOrEmpty ? null : (string?)previous;
    }

    /// <returns>true if this token was still the current connection (and is now cleared).</returns>
    public Task<bool> ClearConnectionIfCurrentAsync(string playerId, string token) =>
        CompareAndDeleteAsync(ConnKey(playerId), token);

    public Task<bool> IsConnectedAsync(string playerId) => Db.KeyExistsAsync(ConnKey(playerId));

    // ───────────── reconnect grace (sorted set: member = player, score = deadline in unix ms) ─────────────

    public Task ScheduleGraceAsync(string playerId, TimeSpan grace) =>
        Db.SortedSetAddAsync(GraceKey, playerId, time.GetUtcNow().Add(grace).ToUnixTimeMilliseconds());

    public Task CancelGraceAsync(string playerId) => Db.SortedSetRemoveAsync(GraceKey, playerId);

    /// <summary>Atomically claims expired entries: ZREM returns true for exactly one node.</summary>
    public async Task<IReadOnlyList<string>> ClaimExpiredGraceAsync(int max)
    {
        var now = time.GetUtcNow().ToUnixTimeMilliseconds();
        var due = await Db.SortedSetRangeByScoreAsync(GraceKey, double.NegativeInfinity, now, take: max);
        var claimed = new List<string>();
        foreach (var member in due)
            if (await Db.SortedSetRemoveAsync(GraceKey, member))
                claimed.Add((string)member!);
        return claimed;
    }

    // ───────────── backplane ─────────────

    public Task PublishAsync(string playerId, Envelope envelope) =>
        Sub.PublishAsync(Channel(playerId), JsonSerializer.Serialize(envelope, JsonDefaults.Options));

    public Task<ChannelMessageQueue> SubscribeAsync(string playerId) => Sub.SubscribeAsync(Channel(playerId));

    // ───────────── last-resort storage ─────────────

    public Task PushDeadResultAsync(string json) => Db.ListRightPushAsync(DeadResultsKey, json);

    private static string Serialize(GameRoom room) => JsonSerializer.Serialize(room.ToState(), JsonDefaults.Options);
}
