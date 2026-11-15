using GameServer.Data;
using GameServer.Messaging;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;

namespace GameServer.Tests;

public sealed class OutboxInboxTests : IDisposable
{
    private readonly SqliteConnection _conn = new("Data Source=:memory:");

    public OutboxInboxTests()
    {
        _conn.Open();
        using var db = NewDb();
        db.Database.EnsureCreated();
    }

    private AppDbContext NewDb() =>
        new(new DbContextOptionsBuilder<AppDbContext>().UseSqlite(_conn).Options);

    private static GameFinishedEvent Event(string? winner, string reason = "win") =>
        new(Guid.NewGuid(), "ROOM01", "alice", "bob", winner, reason, DateTime.UtcNow);

    [Fact]
    public async Task Recording_a_result_writes_result_and_unprocessed_outbox_row()
    {
        var ev = Event("alice");
        await using (var db = NewDb()) await new GameResultRecorder(db).RecordAsync(ev);

        await using var check = NewDb();
        Assert.Single(check.GameResults);
        var outbox = Assert.Single(check.OutboxMessages);
        Assert.Equal(ev.EventId, outbox.Id);
        Assert.Null(outbox.ProcessedAtUtc);
    }

    [Fact]
    public async Task Recording_the_same_event_twice_is_idempotent()
    {
        var ev = Event("alice");
        await using (var db = NewDb()) await new GameResultRecorder(db).RecordAsync(ev);
        await using (var db = NewDb()) await new GameResultRecorder(db).RecordAsync(ev); // retry after an ambiguous failure

        await using var check = NewDb();
        Assert.Single(check.GameResults);
        Assert.Single(check.OutboxMessages);
    }

    [Fact]
    public async Task Win_gives_three_points_to_winner_and_none_to_loser()
    {
        var ev = Event("alice");
        await using (var db = NewDb()) Assert.True(await new ScoreProjector(db).ProcessAsync(ev.EventId, ev));

        await using var check = NewDb();
        var alice = await check.PlayerScores.FindAsync("alice");
        var bob = await check.PlayerScores.FindAsync("bob");
        Assert.Equal((3, 1, 0), (alice!.Points, alice.Wins, alice.Losses));
        Assert.Equal((0, 0, 1), (bob!.Points, bob.Wins, bob.Losses));
    }

    [Fact]
    public async Task Draw_gives_one_point_each()
    {
        var ev = Event(null, "draw");
        await using (var db = NewDb()) await new ScoreProjector(db).ProcessAsync(ev.EventId, ev);

        await using var check = NewDb();
        Assert.Equal(1, (await check.PlayerScores.FindAsync("alice"))!.Points);
        Assert.Equal(1, (await check.PlayerScores.FindAsync("bob"))!.Points);
    }

    [Fact]
    public async Task Duplicate_delivery_is_applied_only_once()
    {
        var ev = Event("alice");
        await using (var db = NewDb()) Assert.True(await new ScoreProjector(db).ProcessAsync(ev.EventId, ev));
        await using (var db = NewDb()) Assert.False(await new ScoreProjector(db).ProcessAsync(ev.EventId, ev));

        await using var check = NewDb();
        Assert.Equal(3, (await check.PlayerScores.FindAsync("alice"))!.Points);
        Assert.Single(check.InboxMessages);
    }

    public void Dispose() => _conn.Dispose();
}
