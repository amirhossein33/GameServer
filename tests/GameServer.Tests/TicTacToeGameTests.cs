using GameServer.Domain;

namespace GameServer.Tests;

public class TicTacToeGameTests
{
    [Fact]
    public void X_moves_first_and_turns_alternate()
    {
        var g = new TicTacToeGame();
        Assert.Equal(MoveError.NotYourTurn, g.TryMove(Mark.O, 0));
        Assert.Equal(MoveError.None, g.TryMove(Mark.X, 0));
        Assert.Equal(MoveError.NotYourTurn, g.TryMove(Mark.X, 1));
    }

    [Fact]
    public void Rejects_taken_cell_and_out_of_range()
    {
        var g = new TicTacToeGame();
        g.TryMove(Mark.X, 4);
        Assert.Equal(MoveError.CellTaken, g.TryMove(Mark.O, 4));
        Assert.Equal(MoveError.OutOfRange, g.TryMove(Mark.O, 9));
        Assert.Equal(MoveError.OutOfRange, g.TryMove(Mark.O, -1));
    }

    [Fact]
    public void Detects_row_win_and_blocks_further_moves()
    {
        var g = Play((Mark.X, 0), (Mark.O, 3), (Mark.X, 1), (Mark.O, 4), (Mark.X, 2));
        Assert.Equal(Mark.X, g.Winner);
        Assert.True(g.IsOver);
        Assert.Equal(MoveError.GameOver, g.TryMove(Mark.O, 8));
    }

    [Fact]
    public void Detects_diagonal_win()
    {
        var g = Play((Mark.X, 0), (Mark.O, 1), (Mark.X, 4), (Mark.O, 2), (Mark.X, 8));
        Assert.Equal(Mark.X, g.Winner);
    }

    [Fact]
    public void Detects_draw()
    {
        var g = Play((Mark.X, 0), (Mark.O, 1), (Mark.X, 2), (Mark.O, 4), (Mark.X, 3),
                     (Mark.O, 5), (Mark.X, 7), (Mark.O, 6), (Mark.X, 8));
        Assert.True(g.IsDraw);
        Assert.Equal(Mark.None, g.Winner);
    }

    private static TicTacToeGame Play(params (Mark, int)[] moves)
    {
        var g = new TicTacToeGame();
        foreach (var (m, c) in moves) Assert.Equal(MoveError.None, g.TryMove(m, c));
        return g;
    }
}
