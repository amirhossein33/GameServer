using GameServer;
using GameServer.Data;
using GameServer.Hub;
using GameServer.Messaging;
using GameServer.State;
using Microsoft.EntityFrameworkCore;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<GameOptions>(builder.Configuration.GetSection("Game"));
builder.Services.Configure<RabbitMqOptions>(builder.Configuration.GetSection("RabbitMq"));

builder.Services.AddDbContext<AppDbContext>(o =>
    o.UseNpgsql(builder.Configuration.GetConnectionString("Default")));

builder.Services.AddSingleton<IConnectionMultiplexer>(_ =>
{
    var cs = builder.Configuration["Redis:ConnectionString"] ?? "localhost:6379";
    var config = ConfigurationOptions.Parse(cs);
    config.AbortOnConnectFail = false; // keep retrying in the background instead of crashing at startup
    return ConnectionMultiplexer.Connect(config);
});

builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<RedisGameState>();
builder.Services.AddSingleton<GameHub>();
builder.Services.AddSingleton<RabbitMqConnection>();
builder.Services.AddScoped<GameResultRecorder>();
builder.Services.AddScoped<ScoreProjector>();
builder.Services.AddHostedService<OutboxPublisher>();
builder.Services.AddHostedService<ScoreConsumer>();
builder.Services.AddHostedService<GraceSweeper>();

var app = builder.Build();

// Create the schema once, even when several replicas start together (Redis lock) and while Postgres is still booting (retry).
var redis = app.Services.GetRequiredService<RedisGameState>();
await Retry.ExecuteAsync(
    async ct =>
    {
        await using var initLock = await redis.AcquireLockAsync("db-init", TimeSpan.FromSeconds(60), TimeSpan.FromSeconds(60));
        using var scope = app.Services.CreateScope();
        await scope.ServiceProvider.GetRequiredService<AppDbContext>().Database.EnsureCreatedAsync(ct);
    },
    maxAttempts: 15, baseDelay: TimeSpan.FromSeconds(1), maxDelay: TimeSpan.FromSeconds(5),
    onRetry: (ex, attempt, wait) =>
        app.Logger.LogWarning("Database not ready (attempt {Attempt}): {Message}. Retrying in {Wait}", attempt, ex.Message, wait));

app.UseDefaultFiles();
app.UseStaticFiles(); // wwwroot/index.html: tiny demo client
app.UseWebSockets();

// ws://localhost:8080/ws?playerId=alice   (no auth on purpose: out of scope for this task)
app.Map("/ws", async (HttpContext ctx, GameHub hub) =>
{
    var playerId = ctx.Request.Query["playerId"].ToString().Trim();
    if (!ctx.WebSockets.IsWebSocketRequest || playerId.Length is 0 or > 64)
    {
        ctx.Response.StatusCode = StatusCodes.Status400BadRequest;
        return;
    }
    using var socket = await ctx.WebSockets.AcceptWebSocketAsync();
    await hub.HandleConnectionAsync(playerId, socket, ctx.RequestAborted);
});

app.MapGet("/healthz", () => Results.Ok(new { status = "ok", node = Environment.MachineName }));

// Read side of the scoreboard, fed only by the RabbitMQ consumer.
app.MapGet("/scores", async (AppDbContext db) =>
    await db.PlayerScores.AsNoTracking().OrderByDescending(s => s.Points).Take(20).ToListAsync());

app.MapGet("/scores/{playerId}", async (string playerId, AppDbContext db) =>
    await db.PlayerScores.AsNoTracking().FirstOrDefaultAsync(s => s.PlayerId == playerId) is { } s
        ? Results.Ok(s) : Results.NotFound());

app.Run();

public partial class Program;
