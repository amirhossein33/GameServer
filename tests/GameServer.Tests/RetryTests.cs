namespace GameServer.Tests;

public class RetryTests
{
    private static readonly Func<TimeSpan, CancellationToken, Task> NoDelay = (_, _) => Task.CompletedTask;

    [Fact]
    public async Task Succeeds_after_transient_failures()
    {
        var calls = 0;
        await Retry.ExecuteAsync(
            _ => ++calls < 3 ? throw new InvalidOperationException("boom") : Task.CompletedTask,
            maxAttempts: 5, baseDelay: TimeSpan.FromMilliseconds(1), delay: NoDelay);
        Assert.Equal(3, calls);
    }

    [Fact]
    public async Task Gives_up_after_max_attempts_and_throws_last_error()
    {
        var calls = 0;
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => Retry.ExecuteAsync(
            _ => { calls++; throw new InvalidOperationException("always"); },
            maxAttempts: 4, baseDelay: TimeSpan.FromMilliseconds(1), delay: NoDelay));
        Assert.Equal("always", ex.Message);
        Assert.Equal(4, calls);
    }

    [Fact]
    public async Task Does_not_retry_errors_the_predicate_rejects()
    {
        var calls = 0;
        await Assert.ThrowsAsync<ArgumentException>(() => Retry.ExecuteAsync(
            _ => { calls++; throw new ArgumentException("permanent"); },
            maxAttempts: 5, baseDelay: TimeSpan.FromMilliseconds(1),
            shouldRetry: e => e is not ArgumentException, delay: NoDelay));
        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task Reports_each_retry_with_growing_wait()
    {
        var waits = new List<TimeSpan>();
        var calls = 0;
        await Retry.ExecuteAsync(
            _ => ++calls < 4 ? throw new Exception("x") : Task.CompletedTask,
            maxAttempts: 5, baseDelay: TimeSpan.FromMilliseconds(100),
            onRetry: (_, _, wait) => waits.Add(wait), delay: NoDelay);
        Assert.Equal(3, waits.Count);
        Assert.True(waits[0] < waits[1] && waits[1] < waits[2]);
    }

    [Fact]
    public void Backoff_doubles_and_is_capped()
    {
        var b = TimeSpan.FromSeconds(1);
        var cap = TimeSpan.FromSeconds(5);
        Assert.Equal(TimeSpan.FromSeconds(1), Retry.Backoff(1, b, cap, jitter: false));
        Assert.Equal(TimeSpan.FromSeconds(2), Retry.Backoff(2, b, cap, jitter: false));
        Assert.Equal(TimeSpan.FromSeconds(4), Retry.Backoff(3, b, cap, jitter: false));
        Assert.Equal(cap, Retry.Backoff(4, b, cap, jitter: false));
    }
}
