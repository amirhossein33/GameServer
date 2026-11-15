using System.Net.WebSockets;
using System.Text;
using System.Text.Json;

namespace GameServer.Hub;

/// <summary>Wraps a socket; WebSocket allows only one concurrent send, so sends are serialised.</summary>
public sealed class PlayerConnection(WebSocket socket)
{
    private readonly SemaphoreSlim _sendLock = new(1, 1);

    /// <summary>Identifies this particular socket cluster-wide (stored in Redis as the player's current connection).</summary>
    public string Token { get; } = Guid.NewGuid().ToString("N");

    public WebSocket Socket => socket;

    public Task SendAsync(object message) =>
        SendRawAsync(JsonSerializer.Serialize(message, JsonDefaults.Options));

    public async Task SendRawAsync(string json)
    {
        if (socket.State != WebSocketState.Open) return;
        var bytes = Encoding.UTF8.GetBytes(json);

        await _sendLock.WaitAsync();
        try
        {
            if (socket.State == WebSocketState.Open)
                await socket.SendAsync(new ArraySegment<byte>(bytes), WebSocketMessageType.Text, true, CancellationToken.None);
        }
        catch (WebSocketException) { /* peer is gone; disconnect handling will clean up */ }
        catch (ObjectDisposedException) { }
        finally { _sendLock.Release(); }
    }

    public async Task CloseAsync(WebSocketCloseStatus status, string reason)
    {
        try
        {
            if (socket.State is WebSocketState.Open or WebSocketState.CloseReceived)
                await socket.CloseAsync(status, reason, CancellationToken.None);
        }
        catch (WebSocketException) { }
        catch (ObjectDisposedException) { }
    }
}
