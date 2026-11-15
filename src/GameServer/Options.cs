namespace GameServer;

public sealed class GameOptions
{
    public TimeSpan ReconnectGrace { get; set; } = TimeSpan.FromSeconds(30);
}

public sealed class RedisOptions
{
    public string ConnectionString { get; set; } = "localhost:6379";
}

public sealed class RabbitMqOptions
{
    public string Host { get; set; } = "localhost";
    public int Port { get; set; } = 5672;
    public string User { get; set; } = "guest";
    public string Password { get; set; } = "guest";
    public string Exchange { get; set; } = "game.events";
    public string Queue { get; set; } = "game.results";
    public string RoutingKey { get; set; } = "game.finished";
    public string DeadLetterExchange { get; set; } = "game.events.dlx";
    public string DeadLetterQueue { get; set; } = "game.results.dlq";
    public string DeadLetterRoutingKey { get; set; } = "game.failed";
}
