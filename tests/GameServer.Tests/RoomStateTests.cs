using System.Text.Json;
using GameServer.Domain;

namespace GameServer.Tests;

/// <summary>Rooms are stored in Redis as JSON; a round trip must not change behaviour.</summary>
public class RoomStateTests
{
    private static GameRoom RoundTrip(GameRoom room)
    {
        var json = JsonSerializer.Serialize(room.ToState(), JsonDefaults.Options);
        return GameRoom.FromState(JsonSerializer.Deserialize<RoomState>(json, JsonDefaults.Options)!);
    }

    [Fact]
    public void Round_trip_preserves_board_turn_and_players()
    {
        var room = new GameRoom("R1", "alice");
        room.Join("bob");
        room.Move("alice", 4);

        var copy = RoundTrip(room);

        Assert.Equal(room.Snapshot(), copy.Snapshot(), new SnapshotComparer());
        Assert.Equal(MoveError.NotYourTurn, copy.Move("alice", 0).Error); // it is bob's turn
        Assert.Equal(MoveError.CellTaken, copy.Move("bob", 4).Error);
        Assert.Equal(MoveError.None, copy.Move("bob", 0).Error);
    }

    [Fact]
    public void Finished_room_keeps_its_outcome_after_round_trip()
    {
        var room = new GameRoom("R1", "alice");
        room.Join("bob");
        room.Abandon("bob");

        var snap = RoundTrip(room).Snapshot();
        Assert.Equal("finished", snap.Status);
        Assert.Equal("alice", snap.WinnerId);
        Assert.Equal("forfeit", snap.Reason);
    }

    private sealed class SnapshotComparer : IEqualityComparer<RoomSnapshot>
    {
        public bool Equals(RoomSnapshot? a, RoomSnapshot? b) =>
            a is not null && b is not null &&
            (a.RoomId, a.Status, a.PlayerX, a.PlayerO, a.Turn, a.WinnerId, a.Reason) ==
            (b.RoomId, b.Status, b.PlayerX, b.PlayerO, b.Turn, b.WinnerId, b.Reason) &&
            a.Board.SequenceEqual(b.Board);

        public int GetHashCode(RoomSnapshot s) => s.RoomId.GetHashCode();
    }
}
