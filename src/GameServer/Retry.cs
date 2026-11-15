namespace GameServer;

/// <summary>Small dependency-free retry helper: exponential backoff with jitter.</summary>
public static class Retry
{
    public static TimeSpan Backoff(int attempt, TimeSpan baseDelay, TimeSpan maxDelay, bool jitter = true)
    {
        var ms = Math.Min(baseDelay.TotalMilliseconds * Math.Pow(2, attempt - 1), maxDelay.TotalMilliseconds);
        if (jitter) ms *= 0.8 + Random.Shared.NextDouble() * 0.4; // +-20%
        return TimeSpan.FromMilliseconds(ms);
    }

    /// <param name="maxAttempts">Total attempts including the first one.</param>
    /// <param name="shouldRetry">Return false for errors that retrying cannot fix.</param>
    /// <param name="delay">Injectable for tests; defaults to Task.Delay.</param>
    public static async Task ExecuteAsync(
        Func<CancellationToken, Task> action,
        int maxAttempts,
        TimeSpan baseDelay,
        TimeSpan? maxDelay = null,
        Func<Exception, bool>? shouldRetry = null,
        Action<Exception, int, TimeSpan>? onRetry = null,
        Func<TimeSpan, CancellationToken, Task>? delay = null,
        CancellationToken ct = default)
    {
        delay ??= (d, t) => Task.Delay(d, t);
        var cap = maxDelay ?? TimeSpan.FromSeconds(30);

        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await action(ct);
                return;
            }
            catch (Exception ex) when (!ct.IsCancellationRequested
                                       && attempt < maxAttempts
                                       && (shouldRetry?.Invoke(ex) ?? true))
            {
                var wait = Backoff(attempt, baseDelay, cap);
                onRetry?.Invoke(ex, attempt, wait);
                await delay(wait, ct);
            }
        }
    }
}
