# GameServer — بک‌اند Tic-Tac-Toe مولتی‌پلیر (.NET 9)

بازی دو نفره‌ی نوبتی با **WebSocket**، منطق کامل سمت سرور، **Redis** برای اشتراک state بین چند نود، و ثبت نتیجه/امتیاز با **RabbitMQ + Outbox/Inbox** روی **PostgreSQL**. کل استک با یک دستور بالا می‌آید.

## اجرا (همه‌چیز با Docker Compose)

```bash
docker compose up --build
```

| آدرس | چیست |
|---|---|
| http://localhost:8080 | کلاینت دمو (از طریق nginx به یکی از دو replica می‌رسد) |
| http://localhost:8080/scores | جدول امتیازها (پر شده توسط consumer) |
| http://localhost:15672 | RabbitMQ UI (guest / guest) |

دو تب باز کن، با دو `playerId` متفاوت وصل شو؛ یکی Create و دیگری Join. پیام `connected` فیلد `node` دارد؛ اگر دو بازیکن روی دو نود مختلف افتادند (container id متفاوت)، بازی باز هم کار می‌کند، چون state در Redis است.

اجرای محلی بدون Docker برای خود برنامه:

```bash
docker compose up -d postgres redis rabbitmq
dotnet run --project src/GameServer
dotnet test tests/GameServer.Tests     # به هیچ سرویس بیرونی نیاز ندارد
```

## پروتکل (JSON روی WebSocket، آدرس `/ws?playerId=<id>`)

| کلاینت → سرور | توضیح |
|---|---|
| `{"type":"create_room"}` | ساخت روم؛ سازنده `X` است و اول بازی می‌کند |
| `{"type":"join_room","roomId":"ABC123"}` | پیوستن به روم؛ بازی شروع می‌شود |
| `{"type":"move","cell":0..8}` | حرکت |

سرور → کلاینت: `connected`, `room_created`, `game_started`, `state`, `game_over`, `reconnected`, `opponent_disconnected` (با `graceSeconds`), `opponent_reconnected`, و `error` با `code` (`not_your_turn`, `cell_taken`, `out_of_range`, `game_over`, `not_started`, `not_in_room`, `room_not_found`, `room_full`, `already_in_room`, `room_busy`, ...).

کلاینت فقط قصد را می‌فرستد؛ اعتبارسنجی، نوبت و تشخیص برنده کاملاً سمت سرور است.

## معماری

```
Client ─► nginx ─► game (replica 1..N, stateless)
                      │
        ┌─────────────┼───────────────────────────────┐
        ▼             ▼                               ▼
      Redis       PostgreSQL                      RabbitMQ
 rooms, locks,   results + outbox + inbox    game.events ─► game.results ─► (DLQ)
 presence, grace,        ▲                           ▲             │
 pub/sub backplane       │                           │             ▼
                         └── OutboxPublisher ────────┘      ScoreConsumer ─► scores
```

### Redis چه کاری می‌کند
| کاربرد | پیاده‌سازی |
|---|---|
| **state روم‌ها** | `game:room:{id}` به‌صورت JSON؛ هر نود روم را load/mutate/save می‌کند، پس هیچ نودی owner خاصی ندارد |
| **قفل توزیع‌شده** | `SET NX PX` + آزادسازی با Lua (compare-and-delete). هر حرکت زیر قفل روم انجام می‌شود، پس race بین دو نود ممکن نیست |
| **عضویت بازیکن** | `game:player-room:{id}` با `SET NX` (هر بازیکن فقط در یک روم) |
| **backplane** | هر نود برای بازیکنانِ متصل به خودش روی `game:player:{id}` subscribe می‌کند؛ ارسال پیام به هر بازیکن = publish روی همان کانال |
| **presence** | `game:conn:{id}` = توکن سوکت فعلی؛ اتصال جدید (روی هر نود) سوکت قبلی را با پیام `replace` می‌بندد |
| **مهلت reconnect** | sorted set `game:grace` (score = زمان انقضا). `GraceSweeper` روی هر نود ورودی‌های منقضی را با `ZREM` به‌صورت atomic claim می‌کند؛ فقط یک نود برنده می‌شود، و حتی اگر نودی که قطع شدن را دید بمیرد، مهلت اجرا می‌شود |
| **هماهنگی** | قفل `outbox-publisher` (فقط یک نود publish می‌کند) و `db-init` (ساخت schema فقط یک بار) |
| **صندوق اضطراری** | لیست `game:dead-results` برای نتایجی که بعد از همه‌ی retryها در DB ذخیره نشدند |

### Retry
| کجا | رفتار |
|---|---|
| ذخیره‌ی نتیجه‌ی بازی | ۵ تلاش با exponential backoff + jitter؛ idempotent (با `EventId`)؛ در نهایت اگر DB پایین بود، در Redis پارک می‌شود و لاگ `Critical` می‌خورد |
| `OutboxPublisher` | publish ناموفق ⇒ `Attempts++` و `NextAttemptAtUtc` با backoff (۱s, ۲s, ۴s … تا سقف ۱ دقیقه)؛ پیام هرگز حذف نمی‌شود؛ قطع RabbitMQ ⇒ reconnect loop |
| `ScoreConsumer` | ۴ تلاش درون‌پردازشی با backoff (مثلاً conflict همزمانی یا خطای گذرای DB)؛ بعد از آن `reject` بدون requeue ⇒ **Dead Letter Queue** (`game.results.dlq`) به‌جای حلقه‌ی بی‌نهایت |
| راه‌اندازی | اتصال به DB تا ۱۵ بار retry می‌شود (Postgres ممکن است دیرتر بالا بیاید)؛ Redis با `AbortOnConnectFail=false` در پس‌زمینه reconnect می‌کند |

### Outbox / Inbox
- **Outbox:** نتیجه و ردیف outbox با یک `SaveChanges` (یک تراکنش) نوشته می‌شوند. مسیر بازی با RabbitMQ کاری ندارد، پس قطع broker بازی را خراب نمی‌کند. publish با publisher confirms است و فقط بعد از ack بروکر، ردیف processed می‌شود (at-least-once).
- **Inbox:** `MessageId` (= شناسه outbox) کلید اصلی جدول inbox است. ردیف inbox و تغییر امتیاز در یک تراکنش commit می‌شوند، پس تحویل تکراری فقط یک بار اثر می‌گذارد. `PlayerScore.Version` یک concurrency token است تا دو نود نتوانند همزمان امتیاز یک بازیکن را بی‌صدا overwrite کنند (در تعارض، retry می‌شود).
- **امتیاز:** برد ۳، مساوی ۱ برای هر دو، باخت ۰؛ `forfeit` برای حریف برد است.

### قطع و وصل
قطع اتصال ⇒ حریف `opponent_disconnected` می‌گیرد و **۳۰ ثانیه** (`Game__ReconnectGrace`) فرصت reconnect با همان `playerId` هست؛ reconnect (به هر نودی) وضعیت روم را دوباره می‌فرستد. در غیر این صورت حریف برنده می‌شود. روم منتظرِ حریف با قطع سازنده بدون ثبت نتیجه لغو می‌شود.

## ساختار پروژه
```
src/GameServer/Domain      قوانین خالص بازی و روم (بدون I/O)
src/GameServer/State       Redis: state، قفل، presence، backplane، GraceSweeper
src/GameServer/Hub         WebSocket و منطق پیام‌ها
src/GameServer/Messaging   Outbox، Inbox، publisher، consumer، RabbitMQ topology
src/GameServer/Data        EF Core (PostgreSQL)
tests/GameServer.Tests     xUnit (SQLite in-memory برای تست Outbox/Inbox)
deploy/nginx.conf          load balancer با پشتیبانی WebSocket
```

## محدودیت‌ها و تصمیم‌های آگاهانه
- احراز هویت ندارد: `playerId` را کلاینت می‌دهد.
- Pub/Sub ردیس at-most-once است؛ اگر پیامی بین دو نود گم شود، کلاینت با reconnect وضعیت درست را می‌گیرد.
- اگر نودی کرش کند، presence بازیکنان آن نود تا TTL (۲۴ ساعت) باقی می‌ماند و مهلت reconnect برایشان شروع نمی‌شود؛ راه‌حل production: heartbeat با TTL کوتاه روی `game:conn:*`.
- Redis تک‌نود است (بدون Sentinel/Cluster) و قفل ساده است (نه Redlock).
- نتایج DLQ دستی بازبینی/replay می‌شوند. اگر تعریف صف عوض شود (مثلاً argument جدید)، صف قدیمی باید پاک شود (`docker compose down -v`).
- تست‌ها: منطق بازی، روم، سریال‌سازی state روم، retry، Outbox و Inbox. تست end-to-end با Redis/RabbitMQ واقعی (مثلاً Testcontainers) اضافه نشده.
- schema با `EnsureCreated` ساخته می‌شود، نه migration.
