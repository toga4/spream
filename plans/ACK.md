# spream Ack メカニズム検討

## 背景

### 問題提起

現在の spream の設計では、Consumer がバッファリングを行った場合に at-least-once 配信保証が困難である。

```go
// 現在の Consumer インターフェース
type Consumer interface {
    Consume(ctx context.Context, change *DataChangeRecord) error
}
```

現在の動作：
- `Consume` が error を返さない → ウォーターマークが更新される（処理完了とみなす）
- `Consume` が error を返す → 全体が停止

**問題**: Consumer 内部でバッファリング（キューイング）を行う場合、`Consume` が return した時点ではまだ処理が完了していない。この状態でウォーターマークが更新されると、クラッシュ時にレコードが失われる。

### Apache Beam の解決策

Apache Beam は **BundleFinalizer** を使用して at-least-once を実現している（詳細は ARCHITECTURE.md 2.6 節参照）。

```
[レコード処理] → [出力] → [バンドルコミット成功] → [ウォーターマーク更新]
                                                    ↑
                                     この時点で初めて「処理完了」
```

spream は単一プロセスのライブラリであり、Beam のような分散ランナーの概念がないため、同等の仕組みを独自に設計する必要がある。

---

## 設計案

### 案1: ack 関数を引数として渡す

Consumer に ack 関数を渡し、処理完了時に呼び出してもらう。

```go
type Consumer interface {
    Consume(ctx context.Context, change *DataChangeRecord, ack func()) error
}
```

**spream 側の実装:**

```go
func (r *partitionReader) processRecord(ctx context.Context, record *DataChangeRecord) error {
    var acked bool
    ack := func() {
        acked = true
    }

    if err := r.consumer.Consume(ctx, record, ack); err != nil {
        return err
    }

    // ack() が呼ばれた場合のみウォーターマークを更新
    if acked {
        if err := r.storage.UpdateWatermark(ctx, r.partition.PartitionToken, record.CommitTimestamp); err != nil {
            return fmt.Errorf("update watermark: %w", err)
        }
    }

    return nil
}
```

**Consumer 側の実装例:**

```go
// 同期処理
func (c *SyncConsumer) Consume(ctx context.Context, change *spream.DataChangeRecord, ack func()) error {
    if err := c.saveToDatabase(ctx, change); err != nil {
        return err  // ack しない → リトライされる
    }
    ack()  // 処理完了を明示
    return nil
}

// 非同期処理（バッファリング）
type BufferedConsumer struct {
    buffer chan workItem
}

type workItem struct {
    record *spream.DataChangeRecord
    ack    func()
}

func (c *BufferedConsumer) Consume(ctx context.Context, change *spream.DataChangeRecord, ack func()) error {
    select {
    case c.buffer <- workItem{record: change, ack: ack}:
        return nil  // バッファに入れた時点で return（まだ ack しない）
    case <-ctx.Done():
        return ctx.Err()
    }
}

func (c *BufferedConsumer) worker(ctx context.Context) {
    for item := range c.buffer {
        if err := c.process(ctx, item.record); err == nil {
            item.ack()  // 処理成功時のみ ack
        }
    }
}
```

**評価:**
| 観点 | 評価 |
|------|------|
| シンプルさ | ○ 関数一つ追加のみ |
| 非同期対応 | ○ ack 関数を保持して後から呼べる |
| 循環依存 | なし |
| 誤用リスク | ack 忘れの可能性 |
| API 変更 | 破壊的変更（シグネチャ変更） |

---

### 案2: Message 構造体でラップ（Pub/Sub スタイル）

Google Cloud Pub/Sub のメッセージ API に似た設計。

```go
type Message struct {
    *DataChangeRecord
    ackFunc func()
}

func (m *Message) Ack() {
    if m.ackFunc != nil {
        m.ackFunc()
    }
}

type Consumer interface {
    Consume(ctx context.Context, msg *Message) error
}
```

**Consumer 側の実装例:**

```go
func (c *MyConsumer) Consume(ctx context.Context, msg *spream.Message) error {
    if err := c.process(ctx, msg.DataChangeRecord); err != nil {
        return err
    }
    msg.Ack()
    return nil
}
```

**評価:**
| 観点 | 評価 |
|------|------|
| シンプルさ | ○ Pub/Sub に馴染みがあれば直感的 |
| 非同期対応 | ○ Message を保持して後から Ack() |
| 循環依存 | なし |
| 誤用リスク | Ack 忘れの可能性 |
| API 変更 | 破壊的変更（型変更） |

---

### 案3: (Ack, error) を返す（Promise/Future パターン）

Consumer が処理完了を追跡する Ack オブジェクトを返す。

```go
type Ack interface {
    // Done は処理完了時に呼ぶ
    Done()
    // Err は処理失敗時に呼ぶ
    Err(error)
}

type Consumer interface {
    // error: Consume 呼び出し自体の失敗
    // Ack: 非同期処理の完了追跡用
    Consume(ctx context.Context, change *DataChangeRecord) (Ack, error)
}
```

**spream 側の実装:**

```go
func (r *partitionReader) processRecord(ctx context.Context, record *DataChangeRecord) error {
    ack, err := r.consumer.Consume(ctx, record)
    if err != nil {
        return err
    }

    // Ack の完了を待つ
    if err := ack.wait(ctx); err != nil {
        return err
    }

    return r.storage.UpdateWatermark(ctx, r.partition.PartitionToken, record.CommitTimestamp)
}
```

**Consumer 側の実装例:**

```go
// 同期処理
func (c *SyncConsumer) Consume(ctx context.Context, change *spream.DataChangeRecord) (spream.Ack, error) {
    if err := c.process(change); err != nil {
        return nil, err
    }
    return spream.ImmediateAck(), nil  // 即座に完了を示す Ack
}

// 非同期処理
func (c *AsyncConsumer) Consume(ctx context.Context, change *spream.DataChangeRecord) (spream.Ack, error) {
    ack := spream.NewAck()

    c.queue <- workItem{
        record: change,
        ack:    ack,
    }

    return ack, nil  // 処理はまだ完了していないが、Ack を返す
}

func (c *AsyncConsumer) worker() {
    for item := range c.queue {
        if err := c.process(item.record); err != nil {
            item.ack.Err(err)
        } else {
            item.ack.Done()
        }
    }
}
```

**Ack の実装例（spream 提供）:**

```go
type ack struct {
    done chan struct{}
    err  error
    once sync.Once
}

func NewAck() Ack {
    return &ack{done: make(chan struct{})}
}

func ImmediateAck() Ack {
    a := &ack{done: make(chan struct{})}
    close(a.done)
    return a
}

func (a *ack) Done() {
    a.once.Do(func() {
        close(a.done)
    })
}

func (a *ack) Err(err error) {
    a.once.Do(func() {
        a.err = err
        close(a.done)
    })
}

func (a *ack) wait(ctx context.Context) error {
    select {
    case <-a.done:
        return a.err
    case <-ctx.Done():
        return ctx.Err()
    }
}
```

**評価:**
| 観点 | 評価 |
|------|------|
| シンプルさ | △ 概念が複雑 |
| 非同期対応 | ○ Ack を保持して後から Done() |
| 循環依存 | なし |
| 誤用リスク | Done()/Err() 忘れの可能性 |
| API 変更 | 破壊的変更 |
| 実装複雑度 | 高（spream 側で wait が必要） |

**問題点:**
- spream は各 Ack の完了を待つ必要があり、実装が複雑化
- 並列処理する場合、複数の Ack を同時に追跡する必要がある
- ウォーターマークの更新順序の管理が複雑

---

### 案4: spream 側で ack を管理（AckTracker）

Consumer のシグネチャを変えず、spream 側で ack を管理する。

```go
// Consumer は変更なし
type Consumer interface {
    Consume(ctx context.Context, change *DataChangeRecord) error
}

// DataChangeRecord に ID を追加
type DataChangeRecord struct {
    // ...
    id string  // spream が内部で生成する一意の ID
}

func (r *DataChangeRecord) ID() string {
    return r.id
}

// Subscriber に Ack/Nack メソッドを追加
func (s *Subscriber) Ack(recordID string) error
func (s *Subscriber) Nack(recordID string) error
```

**spream 側の実装:**

```go
type ackTracker struct {
    mu      sync.Mutex
    pending map[string]*pendingRecord
}

type pendingRecord struct {
    record    *DataChangeRecord
    partition string
    acked     chan struct{}
    nacked    chan struct{}
}

func (r *partitionReader) processRecord(ctx context.Context, record *DataChangeRecord) error {
    record.id = generateID()

    pending := r.ackTracker.add(record, r.partition.PartitionToken)

    if err := r.consumer.Consume(ctx, record); err != nil {
        r.ackTracker.remove(record.id)
        return err
    }

    select {
    case <-pending.acked:
        return r.storage.UpdateWatermark(ctx, r.partition.PartitionToken, record.CommitTimestamp)
    case <-pending.nacked:
        return nil  // リトライ
    case <-time.After(r.config.ackTimeout):
        return fmt.Errorf("ack timeout for record %s", record.id)
    case <-ctx.Done():
        return ctx.Err()
    }
}
```

**Consumer 側の実装例:**

```go
type MyConsumer struct {
    subscriber *spream.Subscriber  // Subscriber への参照が必要
    queue      chan workItem
}

func (c *MyConsumer) Consume(ctx context.Context, change *spream.DataChangeRecord) error {
    c.queue <- workItem{record: change}
    return nil
}

func (c *MyConsumer) worker() {
    for item := range c.queue {
        if err := c.process(item.record); err != nil {
            c.subscriber.Nack(item.record.ID())
        } else {
            c.subscriber.Ack(item.record.ID())
        }
    }
}
```

**評価:**
| 観点 | 評価 |
|------|------|
| シンプルさ | △ ID 管理が必要 |
| 非同期対応 | ○ ID で後から ack |
| 循環依存 | あり（Consumer が Subscriber を参照） |
| 誤用リスク | Ack 忘れでデッドロック |
| API 変更 | 追加 API（既存互換性あり） |
| 実装複雑度 | 高 |

---

### 案5: 現行維持 + ドキュメント明確化

API を変更せず、ドキュメントで責務を明確化する。

```go
// 現行のまま
type Consumer interface {
    Consume(ctx context.Context, change *DataChangeRecord) error
}
```

**ドキュメントで明記:**
- `error` を返さない = 処理完了（ウォーターマーク更新される）
- `error` を返す = 処理失敗（全体が停止）
- バッファリングする場合、at-least-once は Consumer の責任で実装

**評価:**
| 観点 | 評価 |
|------|------|
| シンプルさ | ◎ 変更なし |
| 非同期対応 | × バッファリング時は at-least-once 保証されない |
| API 変更 | なし |
| 誤用リスク | 誤解のリスク |

---

### 案6: AckMode で切り替え可能（ハイブリッド）

既存互換性を維持しつつ、Manual モードを追加。

```go
type AckMode int

const (
    AckModeAuto   AckMode = iota  // error なし = ack（現行動作）
    AckModeManual                  // 明示的に ack() を呼ぶ必要あり
)

// Auto モード用（現行互換）
type Consumer interface {
    Consume(ctx context.Context, change *DataChangeRecord) error
}

// Manual モード用
type ManualAckConsumer interface {
    Consume(ctx context.Context, change *DataChangeRecord, ack func()) error
}

// オプション
func WithAckMode(mode AckMode) Option
```

**使用例:**

```go
// Auto モード（デフォルト、現行互換）
subscriber := spream.NewSubscriber(client, streamName, storage)
subscriber.Subscribe(ctx, &AutoConsumer{})

// Manual モード
subscriber := spream.NewSubscriber(client, streamName, storage,
    spream.WithAckMode(spream.AckModeManual),
)
subscriber.Subscribe(ctx, &ManualConsumer{})
```

**spream 側の実装:**

```go
func (r *partitionReader) processRecord(ctx context.Context, record *DataChangeRecord) error {
    switch r.config.ackMode {
    case AckModeAuto:
        if err := r.consumer.Consume(ctx, record); err != nil {
            return err
        }
        // 自動的にウォーターマーク更新
        return r.storage.UpdateWatermark(ctx, r.partition.PartitionToken, record.CommitTimestamp)

    case AckModeManual:
        var acked bool
        ack := func() { acked = true }

        manualConsumer := r.consumer.(ManualAckConsumer)
        if err := manualConsumer.Consume(ctx, record, ack); err != nil {
            return err
        }

        if acked {
            return r.storage.UpdateWatermark(ctx, r.partition.PartitionToken, record.CommitTimestamp)
        }
        return nil
    }
    return nil
}
```

**評価:**
| 観点 | 評価 |
|------|------|
| シンプルさ | ○ モード切替で対応 |
| 非同期対応 | ○ Manual モードで対応 |
| 既存互換性 | ◎ Auto モードで完全互換 |
| 誤用リスク | Manual モードで ack 忘れ |
| 実装複雑度 | 中 |

---

## 案1〜6 の共通課題: spream 側のバッファリング対応不足

案1〜6 は Consumer 側のバッファリングを想定しているが、**spream 側の実装がバッファリングに対応しきれていない**という根本的な問題がある。

### 問題の詳細

#### 案1/6 の spream 実装の問題

```go
// 案1/6 の spream 側実装
func (r *partitionReader) processRecord(ctx context.Context, record *DataChangeRecord) error {
    var acked bool
    ack := func() { acked = true }

    if err := r.consumer.Consume(ctx, record, ack); err != nil {
        return err
    }

    // 問題: Consume が return した直後に acked をチェックしている
    // Consumer がバッファリングして後から ack() を呼ぶ場合、
    // spream は既に次のレコードに進んでいる
    if acked {
        // ウォーターマーク更新
    }
    return nil
}
```

**問題点:**
1. 非同期 ack を待機する仕組みがない
2. 複数の pending ack を追跡していない
3. 順序外 ack 時のウォーターマーク計算がない

#### 案3 の問題

```go
// 案3 の spream 側実装
ack, err := r.consumer.Consume(ctx, record)
if err != nil { return err }
if err := ack.wait(ctx); err != nil { return err }  // ← 1件ずつ待機
return r.storage.UpdateWatermark(...)
```

これは **1レコードごとに ack を待機** しており、バッファリングの利点（並行処理）が消失する。

### バッファリング対応に必要な要件

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        バッファリング対応の要件                                    │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  1. 複数レコードの同時飛行（in-flight）                                           │
│     - Consumer は複数レコードをバッファに入れ、並行して処理したい                   │
│     - spream は複数の pending ack を追跡する必要がある                            │
│                                                                                 │
│  2. 順序外 ack への対応                                                          │
│     - seq=3 が seq=2 より先に ack される可能性                                   │
│     - ウォーターマークは「連続 ack」されたところまでしか進められない                 │
│                                                                                 │
│  3. バックプレッシャー                                                           │
│     - in-flight レコード数に上限がないと Consumer バッファが溢れる                │
│     - spream 側で in-flight 数を制御する必要がある                               │
│                                                                                 │
│  4. クラッシュ時の復旧                                                           │
│     - ack されていないレコードから再開する必要がある                               │
│     - ウォーターマーク = 連続 ack の最大タイムスタンプ                             │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

### 案7: In-flight Tracker を導入（spream 側でのバッファリング対応）

spream が「飛行中（in-flight）」のレコードを追跡し、連続して ack されたところまでウォーターマークを更新する設計。

#### コンセプト

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           In-flight Tracker の動作                              │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  spream                                                 Consumer                │
│    │                                                       │                    │
│    │── [seq=1, record, ack1] ──────────────────────────────▶│ buffer            │
│    │── [seq=2, record, ack2] ──────────────────────────────▶│ buffer            │
│    │── [seq=3, record, ack3] ──────────────────────────────▶│ buffer            │
│    │                                                       │                    │
│    │   (in-flight: [1,2,3], watermark: 未更新)              │                    │
│    │                                                       │                    │
│    │◀─────────────────────────────────────────── ack3() ───│ seq=3 処理完了     │
│    │   (in-flight: [1,2], seq=3 acked だが連続でない)        │                    │
│    │                                                       │                    │
│    │◀─────────────────────────────────────────── ack1() ───│ seq=1 処理完了     │
│    │   (in-flight: [2], seq=1 acked → watermark=ts1)        │                    │
│    │                                                       │                    │
│    │◀─────────────────────────────────────────── ack2() ───│ seq=2 処理完了     │
│    │   (in-flight: [], seq=1,2,3 連続 → watermark=ts3)      │                    │
│    │                                                       │                    │
└─────────────────────────────────────────────────────────────────────────────────┘
```

#### API 設計

```go
// Consumer インターフェース（案2ベース + 改善）
type Consumer interface {
    Consume(ctx context.Context, msg *Message) error
}

// Message は DataChangeRecord と ack 機能をラップ
type Message struct {
    *DataChangeRecord
    seq     int64       // spream が内部で管理
    ackFunc func()
}

func (m *Message) Ack() {
    if m.ackFunc != nil {
        m.ackFunc()
    }
}

// 新しいオプション
func WithMaxInflight(n int) Option           // 最大 in-flight 数（デフォルト: 1 = 現行同等の同期動作）
func WithAckTimeout(d time.Duration) Option  // ack タイムアウト
```

#### spream 側の実装

```go
// inflightTracker は飛行中のレコードを追跡
type inflightTracker struct {
    mu            sync.Mutex
    cond          *sync.Cond

    pending       map[int64]*inflightEntry  // seq -> entry
    nextSeq       int64                      // 次に発行するシーケンス
    ackedBitmap   map[int64]bool             // seq -> acked
    watermarks    map[int64]time.Time        // seq -> watermark

    maxInflight   int                        // 最大 in-flight 数（バックプレッシャー）

    // 連続 ack の追跡
    lastAckedSeq  int64                      // 連続 ack された最大シーケンス
}

type inflightEntry struct {
    seq       int64
    watermark time.Time
    partition string
}

// waitSlot は in-flight に空きができるまで待機（バックプレッシャー）
func (t *inflightTracker) waitSlot(ctx context.Context) error {
    t.mu.Lock()
    defer t.mu.Unlock()

    for len(t.pending) >= t.maxInflight {
        select {
        case <-ctx.Done():
            return ctx.Err()
        default:
            t.cond.Wait()
        }
    }
    return nil
}

// add は新しいレコードを in-flight に追加し、ack 関数を返す
func (t *inflightTracker) add(partition string, watermark time.Time) (seq int64, ack func()) {
    t.mu.Lock()
    defer t.mu.Unlock()

    seq = t.nextSeq
    t.nextSeq++

    t.pending[seq] = &inflightEntry{
        seq:       seq,
        watermark: watermark,
        partition: partition,
    }
    t.watermarks[seq] = watermark

    ack = func() {
        t.markAcked(seq)
    }

    return seq, ack
}

// markAcked はシーケンスを ack 済みにし、連続 ack をチェック
func (t *inflightTracker) markAcked(seq int64) {
    t.mu.Lock()
    defer t.mu.Unlock()

    t.ackedBitmap[seq] = true

    // 連続 ack の計算
    for {
        nextExpected := t.lastAckedSeq + 1
        if !t.ackedBitmap[nextExpected] {
            break
        }
        t.lastAckedSeq = nextExpected
        delete(t.ackedBitmap, nextExpected)
        delete(t.pending, nextExpected)
    }

    // 空きができたら待機中の goroutine を起こす
    t.cond.Signal()
}

// getSafeWatermark は連続 ack されたウォーターマークを返す
func (t *inflightTracker) getSafeWatermark() time.Time {
    t.mu.Lock()
    defer t.mu.Unlock()

    if t.lastAckedSeq == 0 {
        return time.Time{}
    }
    return t.watermarks[t.lastAckedSeq]
}
```

#### partitionReader での使用

```go
func (r *partitionReader) processRecords(ctx context.Context, records []*changeRecord) error {
    for _, cr := range records {
        for _, record := range cr.DataChangeRecords {
            // バックプレッシャー: in-flight に空きができるまで待機
            if err := r.tracker.waitSlot(ctx); err != nil {
                return err
            }

            // in-flight に追加し、ack 関数を取得
            decoded := record.decodeToNonSpannerType()
            seq, ack := r.tracker.add(r.partition.PartitionToken, record.CommitTimestamp)

            msg := &Message{
                DataChangeRecord: decoded,
                seq:              seq,
                ackFunc:          ack,
            }

            // Consumer に渡す（Consumer はバッファリング可能）
            if err := r.consumer.Consume(ctx, msg); err != nil {
                return err
            }
        }
        // ...
    }

    // 定期的にウォーターマークを更新
    safeWatermark := r.tracker.getSafeWatermark()
    if !safeWatermark.IsZero() {
        if err := r.storage.UpdateWatermark(ctx, r.partition.PartitionToken, safeWatermark); err != nil {
            return err
        }
    }

    return nil
}
```

#### Consumer 側の実装例

```go
// 同期処理（MaxInflight=1 相当）
func (c *SyncConsumer) Consume(ctx context.Context, msg *spream.Message) error {
    if err := c.process(ctx, msg.DataChangeRecord); err != nil {
        return err
    }
    msg.Ack()  // 即座に ack
    return nil
}

// 非同期処理（バッファリング）
type BufferedConsumer struct {
    buffer chan *spream.Message
    wg     sync.WaitGroup
}

func (c *BufferedConsumer) Consume(ctx context.Context, msg *spream.Message) error {
    select {
    case c.buffer <- msg:
        return nil  // バッファに入れて即 return
    case <-ctx.Done():
        return ctx.Err()
    }
}

func (c *BufferedConsumer) worker(ctx context.Context) {
    for msg := range c.buffer {
        if err := c.process(ctx, msg.DataChangeRecord); err == nil {
            msg.Ack()  // 処理成功時のみ ack
        }
        // エラー時は ack しない → リトライされる
    }
}
```

#### 設計上の考慮点

**1. MaxInflight のデフォルト値:**
- `1` をデフォルトにすると現行の同期動作と同等
- 後方互換性を維持しつつ、必要なユーザーは `WithMaxInflight(100)` などで並行処理を有効化

**2. ack タイムアウト:**
```go
// in-flight レコードが一定時間 ack されない場合
func (t *inflightTracker) checkTimeouts(timeout time.Duration) []int64 {
    // タイムアウトしたシーケンスを返す
    // → エラーハンドラーに通知 or 全体停止
}
```

**3. グレースフルシャットダウン:**
```go
func (r *partitionReader) shutdown(ctx context.Context) error {
    // 1. 新規レコードの読み取りを停止
    // 2. in-flight の ack を待機
    if err := r.tracker.waitAllAcked(ctx); err != nil {
        return err
    }
    // 3. 最終ウォーターマークを更新
    // 4. パーティションを FINISHED に
}
```

**評価:**
| 観点 | 評価 |
|------|------|
| シンプルさ | △ 概念は増えるが、責務が明確 |
| 非同期対応 | ◎ 複数 in-flight を正しく追跡 |
| 循環依存 | なし |
| 既存互換性 | △ Message ラップで変更あり（MaxInflight=1 で動作は同等） |
| 誤用リスク | ack 忘れ（タイムアウトで検出可能） |
| spream 実装複雑度 | 高 |
| バックプレッシャー | ◎ maxInflight で制御 |
| 順序外 ack | ◎ 連続 ack で正しくウォーターマーク計算 |

---

## 比較表（更新版）

```
┌──────────────────┬─────────┬─────────┬─────────┬─────────┬─────────┬─────────┬─────────────┐
│                  │ 案1     │ 案2     │ 案3     │ 案4     │ 案5     │ 案6     │ 案7         │
│                  │ ack引数 │ Message │(Ack,err)│AckTrackr│現行維持 │ AckMode │ In-flight   │
├──────────────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────────┤
│ シンプルさ       │ ○       │ ○       │ △       │ △       │ ◎       │ ○       │ △           │
├──────────────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────────┤
│ 非同期対応       │ △※1    │ △※1    │ △※2    │ ○       │ ×       │ △※1    │ ◎           │
├──────────────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────────┤
│ 複数in-flight    │ ×       │ ×       │ ×       │ ×       │ ×       │ ×       │ ◎           │
├──────────────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────────┤
│ 順序外ack        │ ×       │ ×       │ ×       │ ×       │ ×       │ ×       │ ◎           │
├──────────────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────────┤
│ バックプレッシャー│ ×       │ ×       │ ×       │ ×       │ ×       │ ×       │ ◎           │
├──────────────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────────┤
│ 循環依存         │ なし    │ なし    │ なし    │ あり    │ なし    │ なし    │ なし        │
├──────────────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────────┤
│ 既存互換性       │ ×       │ ×       │ ×       │ △       │ ◎       │ ◎       │ △※3        │
├──────────────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────────┤
│ 誤用リスク       │ ack忘れ │ Ack忘れ │ Done忘れ│ Ack忘れ │ 誤解    │ ack忘れ │ ack忘れ     │
│                  │         │         │         │+deadlock│         │(Manual) │ (timeout検出)│
├──────────────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────────┤
│ spream実装複雑度 │ 低      │ 低      │ 高      │ 高      │ なし    │ 中      │ 高          │
└──────────────────┴─────────┴─────────┴─────────┴─────────┴─────────┴─────────┴─────────────┘

※1: Consumer側で非同期処理は可能だが、spream側が待機しないため
     バッファリング時のat-least-once保証が不完全
※2: 1件ずつ待機するためバッファリングの利点（並行処理）が消失
※3: Message ラップで API 変更あり、ただし MaxInflight=1 で動作は現行同等
```

---

## 推奨案（更新版）

### 第一推奨: 案7（In-flight Tracker を導入）

**理由:**
1. **バッファリングを正しくサポート**: 複数の in-flight レコードを追跡し、順序外 ack にも対応
2. **バックプレッシャー**: `maxInflight` で Consumer バッファの溢れを防止
3. **at-least-once 保証**: 連続 ack されたウォーターマークのみ更新するため、クラッシュ時もレコード欠損なし
4. **後方互換性**: `MaxInflight=1` で現行の同期動作と同等の振る舞いを維持可能

**トレードオフ:**
- spream 側の実装複雑度が高い
- API 変更（`*DataChangeRecord` → `*Message`）が必要

### 第二推奨: 案2（Message 構造体でラップ）+ 案7 の簡易版

バッファリング対応が不要な場合、案2 の API で実装を簡略化することも可能。

```go
type Consumer interface {
    Consume(ctx context.Context, msg *Message) error
}

type Message struct {
    *DataChangeRecord
    ackFunc func()
}

func (m *Message) Ack() {
    if m.ackFunc != nil {
        m.ackFunc()
    }
}
```

ただし、この場合は spream 側で 1 件ずつ ack を待機する（案3 相当）か、at-least-once 保証を Consumer 側の責務とするかの選択が必要。

### 非推奨: 案1/6（spream 側のバッファリング対応なし）

案1/6 は Consumer 側のバッファリングを想定した API だが、spream 側が対応していないため、以下の問題がある：
- 非同期 ack を待機しない
- 順序外 ack 時のウォーターマーク計算が不正確
- at-least-once 保証が不完全

### 考慮事項

- 案7 を採用する場合、実装複雑度が高いため、十分なテストと段階的なリリースが必要
- `MaxInflight` のデフォルト値は `1` とし、現行動作との互換性を維持
- ack タイムアウトの検出と通知の仕組みを合わせて実装することで、ack 忘れのリスクを軽減

---

## 未解決の論点

### 案7 で解決される論点

| 論点 | 解決方法 |
|------|----------|
| ack の順序 | 連続 ack の追跡により、順序外 ack でも正しくウォーターマーク計算 |
| バックプレッシャー | `maxInflight` で in-flight 数を制限 |
| 複数 in-flight の追跡 | `inflightTracker` で管理 |

### 残存する論点

1. **ack 忘れの検出方法**:
   - タイムアウト検出（`WithAckTimeout` オプション）で対応可能
   - タイムアウト時の動作: エラー停止 vs リトライ vs ログ警告のみ

2. **Nack の必要性**:
   - リトライを Consumer 側で制御したい場合に必要
   - 案7 では `Ack()` を呼ばなければリトライされるが、明示的な `Nack()` があると意図が明確
   - 追加する場合の API:
     ```go
     func (m *Message) Nack() // 明示的にリトライを要求
     func (m *Message) NackWithDelay(d time.Duration) // 遅延リトライ
     ```

3. **バッチ ack**:
   - 案7 では個別 ack だが、パフォーマンス向上のためバッチ ack が有効な場合がある
   - 検討: `Message.Ack()` を複数回呼ぶのではなく、`Tracker.AckUpTo(seq)` のような API

4. **パーティション分割/マージ時の in-flight 管理**:
   - パーティション終了時、in-flight レコードの ack を待機する必要がある
   - 子パーティションの開始は親パーティションの全 in-flight が ack されてからにすべきか

5. **ウォーターマーク更新頻度**:
   - 案7 では `processRecords` 終了時に更新しているが、より頻繁に更新すべきか
   - 連続 ack のたびに更新 vs 定期的に更新 vs バッチ終了時に更新

6. **エラー時の振る舞い**:
   - `Consume` がエラーを返した場合、in-flight をどう扱うか
   - 即時停止 vs エラーハンドラーに委譲 vs リトライ

### 新たに発生する論点（案7 特有）

1. **シーケンス番号の管理**:
   - パーティション再開時、シーケンス番号をリセットするか継続するか
   - 複数パーティションでシーケンス番号が衝突しないか

2. **メモリ使用量**:
   - `maxInflight` が大きい場合、`watermarks` map のサイズが増大
   - 長時間 ack されないレコードがあるとメモリリークの可能性

3. **デバッグ容易性**:
   - in-flight 状態の可視化（メトリクス、ログ）
   - どのレコードが ack されていないかの追跡

---

## 参考資料

- [Apache Beam BundleFinalizer](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/DoFn.BundleFinalizer.html)
- [Google Cloud Pub/Sub Message.Ack()](https://cloud.google.com/pubsub/docs/reference/libraries)
- ARCHITECTURE.md 2.6 節「BundleFinalizer による At-Least-Once 保証」
