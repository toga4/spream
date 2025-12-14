# spream アーキテクチャ移行レビュー

## エグゼクティブサマリー

本レビューは、ARCHITECTURE.md と IMPLEMENTATION.md で提案されているアーキテクチャ移行について、第三者視点から技術的妥当性を評価し、Go における最適な Change Stream 処理ライブラリの実装を導出するものである。

**結論**: 提案されたアーキテクチャ移行は **概ね妥当** であるが、いくつかの設計上の懸念点と改善提案がある。特に Consumer への context 追加は必須の改善であり、内部アーキテクチャの刷新は適切なアプローチである。ただし、過度な複雑化を避け、Go 言語の特性を活かしたシンプルな設計を維持すべきである。

---

## 1. 現在の実装の評価

### 1.1 強み

| 観点 | 評価 |
|------|------|
| **コード量** | 約300行で Change Stream の完全な読み取りを実現。シンプルで理解しやすい |
| **PartitionStorage の抽象化** | インターフェースによる抽象化が適切。InMemory と Spanner 実装の両方をサポート |
| **状態遷移管理** | CREATED → SCHEDULED → RUNNING → FINISHED の遷移が明確 |
| **リエントラント設計** | Consumer の並行呼び出しに対する注意喚起がドキュメント化されている |

### 1.2 問題点

IMPLEMENTATION.md で指摘された問題点は正確であり、追加で以下の問題も存在する：

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│ 問題の重要度マトリクス                                                            │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  重要度: 高                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │ 1. Consumer に context がない                                           │   │
│  │    - キャンセレーション伝播不可                                           │   │
│  │    - タイムアウト制御不可                                                 │   │
│  │    - Go の慣習に反する                                                   │   │
│  ├─────────────────────────────────────────────────────────────────────────┤   │
│  │ 2. errgroup による全体停止                                               │   │
│  │    - 単一パーティションの一時的エラーで全体が停止                          │   │
│  │    - 復旧の柔軟性がない                                                  │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│  重要度: 中                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │ 3. グレースフルシャットダウンの不完全さ                                   │   │
│  │    - 処理中のレコードの完了を待機する仕組みがない                          │   │
│  │                                                                         │   │
│  │ 4. パーティション検出間隔の固定値                                         │   │
│  │    - 1秒固定、設定による調整不可                                          │   │
│  │                                                                         │   │
│  │ 5. 観測可能性の欠如                                                      │   │
│  │    - メトリクス、トレーシングのフックがない                                │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│  重要度: 低                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │ 6. ウォーターマーク管理の非効率性                                         │   │
│  │    - 毎回 PartitionStorage にクエリが必要                                 │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 1.3 現在の実装の具体的な問題コード

```go
// subscriber.go:319-326 - context が Consumer に渡されていない
func (s *Subscriber) handle(ctx context.Context, p *PartitionMetadata, records []*changeRecord) error {
    var watermarker watermarker
    for _, cr := range records {
        for _, record := range cr.DataChangeRecords {
            // ctx が渡されていない！Consumer がキャンセルを検知できない
            if err := s.consumer.Consume(record.decodeToNonSpannerType()); err != nil {
                return err
            }
            // ...
        }
    }
    // ...
}
```

```go
// subscriber.go:176-180 - errgroup のクロージャが古いパーティションを参照するバグの可能性
for _, p := range interruptedPartitions {
    s.eg.Go(func() error {
        return s.queryChangeStream(ctx, p)  // p はループ変数
    })
}
```

**注**: Go 1.22 以降ではループ変数のキャプチャ動作が変更されたため、このコードは Go 1.24 では安全だが、古いバージョンとの互換性を考慮すると明示的なコピーが望ましい。

---

## 2. 提案されたアーキテクチャの評価

### 2.1 ARCHITECTURE.md の評価

#### 強み

1. **Apache Beam の詳細な分析**: Splittable DoFn、ウォーターマーク、パーティション管理の理解が深い
2. **Go への適切な翻訳**: goroutine と channel を活用した並列処理モデルは Go らしい
3. **責務の明確な分離**: Coordinator、PartitionReader、PartitionScheduler、WatermarkTracker

#### 懸念点

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│ 懸念点 1: RecordMerger の必要性                                                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ARCHITECTURE.md では RecordMerger が設計に含まれているが、                      │
│  IMPLEMENTATION.md では直接 Consumer を呼び出している。                          │
│                                                                                 │
│  分析:                                                                          │
│  - Cloud Spanner Change Stream は「同一キーに対する変更は                        │
│    タイムスタンプ順に同一パーティションから配信される」ことを保証                   │
│  - 異なるパーティション間での順序保証は元々存在しない                              │
│  - したがって、グローバルな順序保証を行う RecordMerger は                         │
│    必ずしも必要ではない                                                          │
│                                                                                 │
│  推奨: RecordMerger は不要。パーティション内での順序は保証されており、             │
│        クロスパーティションの順序保証は Spanner の仕様上不可能                     │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│ 懸念点 2: Channel バッファサイズの設計                                           │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ARCHITECTURE.md では以下のバッファサイズを提案:                                  │
│  - recordChan: 1000                                                             │
│  - partitionEventChan: 100                                                      │
│                                                                                 │
│  問題:                                                                          │
│  - バッファサイズの根拠が不明確                                                  │
│  - Consumer が遅い場合、バッファが溢れる可能性                                   │
│  - 大きすぎるバッファはメモリ圧迫の原因に                                        │
│                                                                                 │
│  分析:                                                                          │
│  現在の実装では iter.Do() のコールバック内で直接 Consumer を呼び出している。      │
│  これは自然なバックプレッシャーを提供する:                                        │
│  - Consumer が遅い → コールバックがブロック → Spanner クエリがブロック           │
│  - これは実は適切な設計である                                                    │
│                                                                                 │
│  推奨: 現在の同期呼び出し方式を維持。バッファリングが必要な場合は                  │
│        Consumer 側で実装すべき（Separation of Concerns）                         │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 IMPLEMENTATION.md の評価

#### 強み

1. **段階的移行計画**: フェーズ分けが現実的
2. **後方互換性の考慮**: LegacyConsumer アダプターの提案
3. **ErrorHandler の設計**: 柔軟なエラーハンドリングを可能に

#### 懸念点と改善提案

##### 2.2.1 partitionScheduler のマージ待機におけるメモリリーク

```go
// IMPLEMENTATION.md:581-630 の設計
type partitionScheduler struct {
    mu sync.Mutex
    pendingMerge map[string]*pendingMergeState
}
```

**問題**: 親パーティションがエラーで終了した場合、pendingMerge に永久に残る可能性がある。

**改善案**:

```go
type partitionScheduler struct {
    mu           sync.Mutex
    pendingMerge map[string]*pendingMergeState
    // タイムアウトでクリーンアップするための追跡
    pendingCreatedAt map[string]time.Time
}

// 定期的なクリーンアップまたは親パーティション終了時のクリーンアップが必要
func (s *partitionScheduler) CleanupStale(timeout time.Duration) {
    s.mu.Lock()
    defer s.mu.Unlock()

    now := time.Now()
    for token, createdAt := range s.pendingCreatedAt {
        if now.Sub(createdAt) > timeout {
            delete(s.pendingMerge, token)
            delete(s.pendingCreatedAt, token)
        }
    }
}
```

##### 2.2.2 ErrorHandler の設計改善

```go
// 現在の提案
type ErrorHandler interface {
    HandleError(ctx context.Context, partition *PartitionMetadata, err error) bool
}
```

**問題**:
- リトライ回数の管理が不明確
- エラーの種類による分岐が Handler 側に押し付けられる

**改善案**:

```go
// より詳細なエラーコンテキスト
type PartitionError struct {
    Partition    *PartitionMetadata
    Err          error
    RetryCount   int
    IsRetryable  bool
    LastRetryAt  time.Time
}

type ErrorHandler interface {
    // OnError はエラー発生時に呼び出される
    // 戻り値: shouldRetry (リトライするか), retryDelay (リトライまでの待機時間)
    OnError(ctx context.Context, err *PartitionError) (shouldRetry bool, retryDelay time.Duration)
}

// デフォルト実装
type DefaultErrorHandler struct {
    MaxRetries     int
    InitialBackoff time.Duration
    MaxBackoff     time.Duration
}
```

##### 2.2.3 config 構造体の肥大化

IMPLEMENTATION.md では config に多くのフィールドが追加される:

```go
type config struct {
    startTimestamp             time.Time
    endTimestamp               time.Time
    heartbeatInterval          time.Duration
    partitionDiscoveryInterval time.Duration  // 新規
    maxConcurrentPartitions    int            // 新規
    errorHandler               ErrorHandler   // 新規
    spannerRequestPriority     spannerpb.RequestOptions_Priority
}
```

**推奨**: Functional Options パターンは維持しつつ、設定のバリデーションを追加:

```go
func (c *config) validate() error {
    if c.partitionDiscoveryInterval < 100*time.Millisecond {
        return errors.New("partitionDiscoveryInterval must be at least 100ms")
    }
    if c.maxConcurrentPartitions < 0 {
        return errors.New("maxConcurrentPartitions must be non-negative")
    }
    if c.startTimestamp.After(c.endTimestamp) {
        return errors.New("startTimestamp must be before endTimestamp")
    }
    return nil
}
```

---

## 3. Go における最適な Change Stream 処理ライブラリの設計

### 3.1 設計原則

Go 言語の特性と Cloud Spanner Change Stream の性質を考慮し、以下の原則を提唱する:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           設計原則                                               │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  1. シンプルさの維持 (Simplicity)                                               │
│     - 過度な抽象化を避ける                                                       │
│     - 理解しやすく、デバッグしやすいコード                                        │
│     - "A little copying is better than a little dependency"                     │
│                                                                                 │
│  2. 明示的な制御フロー (Explicit Control Flow)                                   │
│     - context.Context による明示的なキャンセレーション                            │
│     - エラーは明示的に返す                                                       │
│     - 隠れた状態変更を避ける                                                     │
│                                                                                 │
│  3. 自然なバックプレッシャー (Natural Backpressure)                              │
│     - Consumer の処理速度に自然に追従                                            │
│     - 明示的なレート制限は Consumer 側の責務                                      │
│                                                                                 │
│  4. 拡張可能性 (Extensibility)                                                  │
│     - インターフェースによる拡張ポイント                                          │
│     - Functional Options パターン                                               │
│                                                                                 │
│  5. 観測可能性 (Observability)                                                  │
│     - メトリクス、トレーシングのフック                                            │
│     - ログ出力のカスタマイズ                                                     │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 推奨アーキテクチャ

提案された coordinator パターンを基に、以下の修正を加えた設計を推奨する:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              推奨アーキテクチャ                                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                           Subscriber (公開)                             │   │
│  │  - Subscribe(ctx, Consumer) error                                       │   │
│  │  - SubscribeFunc(ctx, ConsumerFunc) error                               │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                        │                                        │
│                                        ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                         coordinator (内部)                              │   │
│  │                                                                         │   │
│  │  責務:                                                                  │   │
│  │  - パーティション検出ループ                                              │   │
│  │  - partitionReader の起動・停止                                         │   │
│  │  - グレースフルシャットダウン                                            │   │
│  │                                                                         │   │
│  │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐      │   │
│  │  │ partitionReader  │  │ partitionReader  │  │ partitionReader  │      │   │
│  │  │   (goroutine)    │  │   (goroutine)    │  │   (goroutine)    │      │   │
│  │  └────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘      │   │
│  │           │                     │                     │                │   │
│  │           └──────────┬──────────┴──────────┬──────────┘                │   │
│  │                      │                     │                           │   │
│  │              partitionEvents         直接呼び出し                       │   │
│  │              (子パーティション、       (Consumer.Consume)               │   │
│  │               終了通知)                                                 │   │
│  │                      │                     │                           │   │
│  │                      ▼                     ▼                           │   │
│  │  ┌──────────────────────────┐   ┌──────────────────────────┐          │   │
│  │  │   partitionScheduler    │   │        Consumer          │          │   │
│  │  │  (マージ待機管理)        │   │      (ユーザー実装)       │          │   │
│  │  └──────────────────────────┘   └──────────────────────────┘          │   │
│  │                                                                         │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│  注: RecordMerger は不要。バッファリングチャネルも不要。                          │
│      自然なバックプレッシャーを活用。                                            │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 3.3 API 設計の推奨

#### 3.3.1 Consumer インターフェース (必須変更)

```go
// Consumer は DataChangeRecord を消費するインターフェース
//
// Consume は複数の goroutine から並行して呼び出される可能性があり、
// 実装はリエントラントかつスレッドセーフでなければならない。
//
// ctx がキャンセルされた場合、Consume は速やかに ctx.Err() を返すべきである。
type Consumer interface {
    Consume(ctx context.Context, change *DataChangeRecord) error
}

type ConsumerFunc func(context.Context, *DataChangeRecord) error

func (f ConsumerFunc) Consume(ctx context.Context, change *DataChangeRecord) error {
    return f(ctx, change)
}
```

#### 3.3.2 新規オプション (推奨追加)

```go
// WithPartitionDiscoveryInterval はパーティション検出の間隔を設定する
// デフォルト: 1秒
// 最小値: 100ミリ秒
func WithPartitionDiscoveryInterval(interval time.Duration) Option

// WithMaxConcurrentPartitions は同時に処理するパーティションの最大数を設定する
// 0 は無制限を意味する
// デフォルト: 0 (無制限)
func WithMaxConcurrentPartitions(max int) Option

// WithErrorHandler はパーティション処理のエラーハンドラーを設定する
// nil の場合、最初のエラーで全体が停止する（現在の動作）
func WithErrorHandler(handler ErrorHandler) Option
```

#### 3.3.3 ErrorHandler インターフェース

```go
// ErrorHandler はパーティション処理中のエラーを処理する
type ErrorHandler interface {
    // HandleError はエラー発生時に呼び出される
    //
    // 戻り値:
    // - shouldContinue: true の場合、他のパーティションの処理を継続
    // - retryDelay: 0 より大きい場合、指定時間後に当該パーティションをリトライ
    //               0 の場合、リトライしない
    HandleError(ctx context.Context, partition *PartitionMetadata, err error) (shouldContinue bool, retryDelay time.Duration)
}
```

### 3.4 内部実装の推奨

#### 3.4.1 coordinator の簡略化

IMPLEMENTATION.md の coordinator は適切だが、以下の簡略化を推奨:

```go
type coordinator struct {
    // 依存
    spannerClient    *spanner.Client
    streamName       string
    partitionStorage PartitionStorage
    consumer         Consumer
    config           *config

    // パーティション管理
    mu      sync.RWMutex
    readers map[string]context.CancelFunc  // token -> cancel function

    // イベント
    events chan partitionEvent

    // 制御
    ctx    context.Context
    cancel context.CancelFunc
    wg     sync.WaitGroup

    // エラー
    err     error
    errOnce sync.Once
}
```

#### 3.4.2 partitionReader の簡略化

```go
func (r *partitionReader) run(ctx context.Context) error {
    if err := r.storage.UpdateToRunning(ctx, r.partition.PartitionToken); err != nil {
        return fmt.Errorf("update to running: %w", err)
    }

    if err := r.readStream(ctx); err != nil {
        // context キャンセルは正常終了として扱う
        if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
            return nil
        }
        return err
    }

    if err := r.storage.UpdateToFinished(ctx, r.partition.PartitionToken); err != nil {
        return fmt.Errorf("update to finished: %w", err)
    }

    return nil
}

func (r *partitionReader) readStream(ctx context.Context) error {
    stmt := r.buildStatement()
    iter := r.client.Single().QueryWithOptions(ctx, stmt, r.queryOptions())

    return iter.Do(func(row *spanner.Row) error {
        records := []*changeRecord{}
        if err := row.Columns(&records); err != nil {
            return err
        }
        return r.processRecords(ctx, records)
    })
}

func (r *partitionReader) processRecords(ctx context.Context, records []*changeRecord) error {
    var latestWatermark time.Time

    for _, cr := range records {
        // DataChangeRecord
        for _, record := range cr.DataChangeRecords {
            // context を渡す（重要な変更点）
            if err := r.consumer.Consume(ctx, record.decodeToNonSpannerType()); err != nil {
                return err
            }
            latestWatermark = maxTime(latestWatermark, record.CommitTimestamp)
        }

        // HeartbeatRecord
        for _, record := range cr.HeartbeatRecords {
            latestWatermark = maxTime(latestWatermark, record.Timestamp)
        }

        // ChildPartitionsRecord
        for _, record := range cr.ChildPartitionsRecords {
            if err := r.storage.AddChildPartitions(ctx, r.partition.EndTimestamp, r.partition.HeartbeatMillis, record); err != nil {
                return fmt.Errorf("add child partitions: %w", err)
            }
            r.notifyChildPartitions(record)
            latestWatermark = maxTime(latestWatermark, record.StartTimestamp)
        }
    }

    if !latestWatermark.IsZero() {
        if err := r.storage.UpdateWatermark(ctx, r.partition.PartitionToken, latestWatermark); err != nil {
            return fmt.Errorf("update watermark: %w", err)
        }
    }

    return nil
}
```

### 3.5 WatermarkTracker の必要性について

ARCHITECTURE.md と IMPLEMENTATION.md では WatermarkTracker を提案しているが、これは**オプショナル**である。

**現在の実装**: PartitionStorage.GetUnfinishedMinWatermarkPartition() で毎回クエリ

**WatermarkTracker のメリット**:
- クエリ回数の削減
- メモリ上での高速なグローバルウォーターマーク計算

**WatermarkTracker のデメリット**:
- メモリ管理の複雑化
- PartitionStorage との状態同期の問題

**推奨**:
- v1 では現在の実装（PartitionStorage への毎回クエリ）を維持
- パフォーマンス問題が顕在化した場合に WatermarkTracker を追加
- PartitionStorage の実装（InMemory, Spanner）によって適切な戦略が異なる

---

## 4. 移行計画の評価と修正提案

### 4.1 フェーズ分けの評価

IMPLEMENTATION.md のフェーズ分けは適切だが、以下の修正を推奨:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           修正された移行計画                                      │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  フェーズ 1: Consumer インターフェースの変更 (破壊的変更)                          │
│  ──────────────────────────────────────────────────────────────────────────     │
│  - Consumer.Consume(ctx, change) への変更                                       │
│  - バージョン: v2.0.0 としてリリース                                             │
│  - 移行ガイドの提供                                                              │
│                                                                                 │
│  フェーズ 2: 内部リファクタリング (非破壊的変更)                                   │
│  ──────────────────────────────────────────────────────────────────────────     │
│  - coordinator パターンの導入                                                    │
│  - partitionReader の分離                                                        │
│  - partitionScheduler の追加                                                     │
│  - バージョン: v2.1.0                                                            │
│                                                                                 │
│  フェーズ 3: 新機能追加 (非破壊的変更)                                            │
│  ──────────────────────────────────────────────────────────────────────────     │
│  - WithPartitionDiscoveryInterval                                               │
│  - WithMaxConcurrentPartitions                                                  │
│  - WithErrorHandler                                                             │
│  - バージョン: v2.2.0                                                            │
│                                                                                 │
│  フェーズ 4: 観測可能性の追加 (非破壊的変更) [オプション]                           │
│  ──────────────────────────────────────────────────────────────────────────     │
│  - メトリクスフック                                                              │
│  - トレーシング統合                                                              │
│  - バージョン: v2.3.0                                                            │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 後方互換性アダプターについて

IMPLEMENTATION.md で提案された LegacyConsumer アダプターは**提供しない**ことを推奨する。

**理由**:
1. メジャーバージョンアップ（v2）として破壊的変更を明示
2. アダプターは一時的な便宜であり、長期的にはメンテナンス負荷
3. Go のセマンティックバージョニングに従えば、v2 モジュールパス変更で明確に分離可能

**移行支援**:
- 詳細な移行ガイドをドキュメントに追加
- sed/gofmt によるコード変換スクリプトの提供

---

## 5. 代替アプローチの検討

### 5.1 Pull 型 API (Iterator パターン)

```go
// 代替案: Pull 型 API
type Reader struct {
    // ...
}

func (r *Reader) Next(ctx context.Context) (*DataChangeRecord, error) {
    // ...
}

// 使用例
for {
    record, err := reader.Next(ctx)
    if err == io.EOF {
        break
    }
    if err != nil {
        return err
    }
    // record を処理
}
```

**メリット**:
- 制御フローが呼び出し側にある
- バックプレッシャーが自然
- テストが容易

**デメリット**:
- 複数パーティションからの読み取りの抽象化が困難
- 内部状態管理が複雑化

**結論**: Change Stream の複数パーティション並列処理の性質上、Push 型（現在のアプローチ）が適切。

### 5.2 Channel ベース API

```go
// 代替案: Channel ベース API
func (s *Subscriber) Subscribe(ctx context.Context) (<-chan *DataChangeRecord, <-chan error) {
    records := make(chan *DataChangeRecord)
    errs := make(chan error, 1)

    go func() {
        defer close(records)
        defer close(errs)
        // ...
    }()

    return records, errs
}

// 使用例
records, errs := subscriber.Subscribe(ctx)
for {
    select {
    case record, ok := <-records:
        if !ok {
            return nil
        }
        // record を処理
    case err := <-errs:
        return err
    }
}
```

**メリット**:
- Go らしい API
- select による柔軟な制御

**デメリット**:
- エラーハンドリングが複雑
- バッファリングの問題が再発

**結論**: Consumer インターフェースの方がシンプルで、エラーハンドリングが明確。

---

## 6. 結論と推奨事項

### 6.1 総合評価

| 観点 | 評価 | コメント |
|------|------|----------|
| **Consumer への context 追加** | ✅ 必須 | Go の慣習に従い、キャンセレーション制御を可能に |
| **coordinator パターン** | ✅ 推奨 | 責務分離により保守性向上 |
| **partitionScheduler** | ✅ 推奨 | マージ待機のロジック分離 |
| **WatermarkTracker** | ⚠️ 保留 | 現時点では不要、必要に応じて追加 |
| **RecordMerger** | ❌ 不要 | パーティション内順序で十分 |
| **バッファリングチャネル** | ❌ 不要 | 自然なバックプレッシャーを活用 |
| **ErrorHandler** | ✅ 推奨 | 柔軟なエラーハンドリング |
| **新規オプション** | ✅ 推奨 | 設定の柔軟性向上 |

### 6.2 実装優先度

```
高優先度 (v2.0.0):
├── Consumer.Consume(ctx, change) への変更
└── 既存テストの更新

中優先度 (v2.1.0):
├── coordinator パターンの導入
├── partitionReader の分離
├── partitionScheduler の追加
└── グレースフルシャットダウンの改善

低優先度 (v2.2.0+):
├── WithPartitionDiscoveryInterval
├── WithMaxConcurrentPartitions
├── WithErrorHandler
└── 観測可能性フック
```

### 6.3 最終推奨

1. **IMPLEMENTATION.md の方向性は正しい**。ただし、過度な複雑化を避けるべき。

2. **RecordMerger とバッファリングチャネルは不要**。現在の同期呼び出し方式を維持し、自然なバックプレッシャーを活用する。

3. **段階的な移行を推奨**。まず Consumer への context 追加を v2.0.0 としてリリースし、内部リファクタリングは後続バージョンで行う。

4. **WatermarkTracker は必要に応じて追加**。現時点では PartitionStorage への毎回クエリで十分。

5. **テストカバレッジの向上が必要**。特に、coordinator と partitionReader の単体テスト、パーティション分割/マージのシナリオテストを追加すべき。

---

## 付録: 参考実装のコードスケルトン

### A.1 Consumer インターフェース

```go
// consumer.go

package spream

import "context"

// Consumer は DataChangeRecord を消費するインターフェース
type Consumer interface {
    Consume(ctx context.Context, change *DataChangeRecord) error
}

// ConsumerFunc は関数を Consumer として使用するためのアダプタ
type ConsumerFunc func(context.Context, *DataChangeRecord) error

func (f ConsumerFunc) Consume(ctx context.Context, change *DataChangeRecord) error {
    return f(ctx, change)
}
```

### A.2 ErrorHandler インターフェース

```go
// error_handler.go

package spream

import (
    "context"
    "time"
)

// ErrorHandler はパーティション処理中のエラーを処理する
type ErrorHandler interface {
    HandleError(ctx context.Context, partition *PartitionMetadata, err error) (shouldContinue bool, retryDelay time.Duration)
}

// ErrorHandlerFunc は関数を ErrorHandler として使用するためのアダプタ
type ErrorHandlerFunc func(context.Context, *PartitionMetadata, error) (bool, time.Duration)

func (f ErrorHandlerFunc) HandleError(ctx context.Context, partition *PartitionMetadata, err error) (bool, time.Duration) {
    return f(ctx, partition, err)
}

// DefaultErrorHandler はデフォルトのエラーハンドラ（最初のエラーで停止）
var DefaultErrorHandler = ErrorHandlerFunc(func(_ context.Context, _ *PartitionMetadata, _ error) (bool, time.Duration) {
    return false, 0
})
```

---

*このレビューは 2025年12月14日 に作成されました。*
