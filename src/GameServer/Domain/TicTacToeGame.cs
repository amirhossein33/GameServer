namespace GameServer.Domain;

public enum Mark { None, X, O }

public enum MoveError { None, GameOver, NotStarted, NotInRoom, NotYourTurn, OutOfRange, CellTaken }

/// <summary>Pure game rules. No I/O, no threading: callers serialise access (room lock).</summary>
public sealed class TicTacToeGame
{
    private static readonly int[][] Lines =
        [[0, 1, 2], [3, 4, 5], [6, 7, 8], [0, 3, 6], [1, 4, 7], [2, 5, 8], [0, 4, 8], [2, 4, 6]];

    private readonly Mark[] _board = new Mark[9];

    public Mark Turn { get; private set; } = Mark.X;
    public Mark Winner { get; private set; } = Mark.None;
    public bool IsDraw { get; private set; }
    public bool IsOver => Winner != Mark.None || IsDraw;
    public IReadOnlyList<Mark> Board => _board;

    public static TicTacToeGame Restore(IReadOnlyList<Mark> board, Mark turn, Mark winner, bool isDraw)
    {
        if (board.Count != 9) throw new ArgumentException("Board must have 9 cells", nameof(board));
        var game = new TicTacToeGame { Turn = turn, Winner = winner, IsDraw = isDraw };
        for (var i = 0; i < 9; i++) game._board[i] = board[i];
        return game;
    }

    public MoveError TryMove(Mark player, int cell)
    {
        if (IsOver) return MoveError.GameOver;
        if (cell is < 0 or > 8) return MoveError.OutOfRange;
        if (player != Turn) return MoveError.NotYourTurn;
        if (_board[cell] != Mark.None) return MoveError.CellTaken;

        _board[cell] = player;

        if (Lines.Any(l => l.All(i => _board[i] == player))) Winner = player;
        else if (_board.All(c => c != Mark.None)) IsDraw = true;
        else Turn = player == Mark.X ? Mark.O : Mark.X;

        return MoveError.None;
    }
}
