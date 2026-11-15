using GameServer.Domain;

namespace GameServer.Tests;

public class GameRoomTests
{
    private static GameRoom Started()
    {
        var room = new GameRoom("R1", "alice");
        Assert.Equal(JoinError.None, room.Join("bob"));
        return room;
    }

    [Fact]
    public void Cannot_move_before_opponent_joins()
    {
        var room = new GameRoom("R1", "alice");
        Assert.Equal(MoveError.NotStarted, room.Move("alice", 0).Error);
    }

    [Fact]
    public void Creator_cannot_join_own_room_and_third_player_is_rejected()
    {
        var room = new GameRoom("R1", "alice");
        Assert.Equal(JoinError.AlreadyInRoom, room.Join("alice"));
        room.Join("bob");
        Assert.Equal(JoinError.RoomFull, room.Join("carol"));
    }

    [Fact]
    public void Out_of_turn_and_stranger_moves_are_rejected()
    {
        var room = Started();
        Assert.Equal(MoveError.NotYourTurn, room.Move("bob", 0).Error);
        Assert.Equal(MoveError.NotInRoom, room.Move("mallory", 0).Error);
    }

    [Fact]
    public void Winning_move_returns_outcome_once()
    {
        var room = Started();
        room.Move("alice", 0); room.Move("bob", 3);
        room.Move("alice", 1); room.Move("bob", 4);
        var last = room.Move("alice", 2);

        Assert.Equal(new GameOutcome("alice", "win"), last.Outcome);
        Assert.Equal(MoveError.GameOver, room.Move("bob", 5).Error);
        Assert.Equal(AbandonResult.NoOp, room.Abandon("bob")); // cannot finish twice
    }

    [Fact]
    public void Abandoning_mid_game_forfeits_to_opponent()
    {
        var room = Started();
        Assert.Equal(AbandonResult.Finished, room.Abandon("alice"));
        var snap = room.Snapshot();
        Assert.Equal("bob", snap.WinnerId);
        Assert.Equal("forfeit", snap.Reason);
    }

    [Fact]
    public void Abandoning_waiting_room_cancels_without_result()
    {
        var room = new GameRoom("R1", "alice");
        Assert.Equal(AbandonResult.Cancelled, room.Abandon("alice"));
        Assert.Equal(JoinError.NotJoinable, room.Join("bob"));
    }
}
