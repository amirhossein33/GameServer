namespace GameServer.Domain;

public enum RoomStatus { Waiting, InProgress, Finished, Cancelled }
public enum JoinError { None, RoomFull, AlreadyInRoom, NotJoinable }
public enum AbandonResult { NoOp, Cancelled, Finished }

public sealed record GameOutcome(string? WinnerId, string Reason);
public sealed record MoveResult(MoveError Error, GameOutcome? Outcome);
public sealed record RoomSnapshot(
    string RoomId, string Status, string? PlayerX, string? PlayerO,
    string[] Board, string? Turn, string? WinnerId, string? Reason);

/// <summary>Serialisable room state, stored in Redis so any node can serve any room.</summary>
public sealed record RoomState(
    string Id, string XId, string? OId, RoomStatus Status,
    Mark[] Board, Mark Turn, Mark Winner, bool IsDraw, string? WinnerId, string? Reason);

/// <summary>
/// One room = two players + one game. The object is loaded from Redis, mutated while the
/// distributed room lock is held, and saved back. The in-process lock only protects the object itself.
/// </summary>
public sealed class GameRoom
{
    private readonly object _gate = new();
    private readonly TicTacToeGame _game;
    private readonly string _xId;
    private string? _oId;
    private RoomStatus _status;
    private GameOutcome? _outcome;

    public string Id { get; }

    public GameRoom(string id, string creatorId)
    {
        Id = id;
        _xId = creatorId;
        _game = new TicTacToeGame();
        _status = RoomStatus.Waiting;
    }

    private GameRoom(RoomState s)
    {
        Id = s.Id;
        _xId = s.XId;
        _oId = s.OId;
        _status = s.Status;
        _game = TicTacToeGame.Restore(s.Board, s.Turn, s.Winner, s.IsDraw);
        _outcome = s.Reason is null ? null : new GameOutcome(s.WinnerId, s.Reason);
    }

    public static GameRoom FromState(RoomState state) => new(state);

    public RoomState ToState()
    {
        lock (_gate)
        {
            return new RoomState(Id, _xId, _oId, _status, _game.Board.ToArray(), _game.Turn,
                _game.Winner, _game.IsDraw, _outcome?.WinnerId, _outcome?.Reason);
        }
    }

    public JoinError Join(string playerId)
    {
        lock (_gate)
        {
            if (playerId == _xId || playerId == _oId) return JoinError.AlreadyInRoom;
            if (_status == RoomStatus.InProgress) return JoinError.RoomFull;
            if (_status != RoomStatus.Waiting) return JoinError.NotJoinable;
            _oId = playerId;
            _status = RoomStatus.InProgress;
            return JoinError.None;
        }
    }

    public MoveResult Move(string playerId, int cell)
    {
        lock (_gate)
        {
            if (_status == RoomStatus.Waiting) return new(MoveError.NotStarted, null);
            if (_status != RoomStatus.InProgress) return new(MoveError.GameOver, null);

            var mark = playerId == _xId ? Mark.X : playerId == _oId ? Mark.O : Mark.None;
            if (mark == Mark.None) return new(MoveError.NotInRoom, null);

            var error = _game.TryMove(mark, cell);
            if (error != MoveError.None) return new(error, null);

            if (_game.Winner != Mark.None)
                Finish(_game.Winner == Mark.X ? _xId : _oId, "win");
            else if (_game.IsDraw)
                Finish(null, "draw");

            return new(MoveError.None, _outcome);
        }
    }

    /// <summary>Called when a player's reconnect grace period expires.</summary>
    public AbandonResult Abandon(string leaverId)
    {
        lock (_gate)
        {
            if (_status == RoomStatus.Waiting && leaverId == _xId)
            {
                _status = RoomStatus.Cancelled;
                return AbandonResult.Cancelled;
            }
            if (_status == RoomStatus.InProgress && (leaverId == _xId || leaverId == _oId))
            {
                Finish(leaverId == _xId ? _oId : _xId, "forfeit");
                return AbandonResult.Finished;
            }
            return AbandonResult.NoOp;
        }
    }

    public IReadOnlyList<string> PlayerIds()
    {
        lock (_gate) return _oId is null ? [_xId] : [_xId, _oId];
    }

    public RoomSnapshot Snapshot()
    {
        lock (_gate)
        {
            return new RoomSnapshot(
                Id, _status.ToString().ToLowerInvariant(), _xId, _oId,
                _game.Board.Select(m => m == Mark.None ? "" : m.ToString()).ToArray(),
                _status == RoomStatus.InProgress ? _game.Turn.ToString() : null,
                _outcome?.WinnerId, _outcome?.Reason);
        }
    }

    private void Finish(string? winnerId, string reason)
    {
        _status = RoomStatus.Finished;
        _outcome = new GameOutcome(winnerId, reason);
    }
}
