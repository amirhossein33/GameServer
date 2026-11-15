using Microsoft.EntityFrameworkCore;

namespace GameServer.Data;

public sealed class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
    public DbSet<GameResultEntity> GameResults => Set<GameResultEntity>();
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();
    public DbSet<InboxMessage> InboxMessages => Set<InboxMessage>();
    public DbSet<PlayerScore> PlayerScores => Set<PlayerScore>();

    protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<GameResultEntity>().Property(x => x.Id).ValueGeneratedNever();
        b.Entity<OutboxMessage>().Property(x => x.Id).ValueGeneratedNever();
        b.Entity<OutboxMessage>().HasIndex(x => new { x.ProcessedAtUtc, x.NextAttemptAtUtc, x.CreatedAtUtc });
        b.Entity<InboxMessage>().HasKey(x => x.MessageId);
        b.Entity<InboxMessage>().Property(x => x.MessageId).ValueGeneratedNever();
        b.Entity<PlayerScore>().HasKey(x => x.PlayerId);
    }
}
