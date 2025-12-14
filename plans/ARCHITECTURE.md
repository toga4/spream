# Cloud Spanner Change Stream Reader アーキテクチャ設計

## 目次

1. [Apache Beam アーキテクチャ概要](#1-apache-beam-アーキテクチャ概要)
2. [Cloud Spanner Change Stream 実装の解析](#2-cloud-spanner-change-stream-実装の解析)
3. [Go での実装アーキテクチャ](#3-go-での実装アーキテクチャ)

---

## 1. Apache Beam アーキテクチャ概要

### 1.1 コア概念

Apache Beam は、分散データ処理パイプラインを構築するための統一プログラミングモデルを提供する。

```
Pipeline
    │
    ├── PCollection  (不変の分散コレクション)
    │       │
    │       └── 各要素は Timestamp と Window を持つ
    │
    ├── PTransform   (変換操作)
    │       │
    │       ├── ParDo      (要素ごとの処理)
    │       ├── GroupByKey (キーによるグループ化)
    │       └── Combine    (集約処理)
    │
    └── Runner       (実行エンジン: Dataflow, Flink, etc.)
```

### 1.2 Splittable DoFn (SDF)

Change Stream 実装の中核となる機能。単一の入力要素を複数の処理ユニット（制限）に分割可能にする。

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Splittable DoFn                             │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  @GetInitialRestriction  ──► 要素の初期制限を定義                   │
│           │                                                         │
│           ▼                                                         │
│  @NewTracker            ──► RestrictionTracker インスタンス生成     │
│           │                                                         │
│           ▼                                                         │
│  @ProcessElement        ──► 制限内で処理実行                        │
│           │                                                         │
│           ├── tryClaim(position)  成功 → 処理続行                   │
│           │                       失敗 → 処理停止                   │
│           │                                                         │
│           └── ProcessContinuation.resume() / stop()                 │
│                                                                     │
│  trySplit(fraction)     ──► 動的分割 (Primary / Residual)          │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 1.3 ウォーターマークとタイムスタンプ

- **タイムスタンプ**: 各要素のイベント発生時刻
- **ウォーターマーク**: 「この時刻までのデータは完全」を示す指標
- Change Stream では `ManualWatermarkEstimator` で明示的に制御

---

## 2. Cloud Spanner Change Stream 実装の解析

### 2.1 全体構成

```
                           ┌──────────────────────────────────────────┐
                           │           SpannerIO.ReadChangeStream     │
                           └─────────────────────┬────────────────────┘
                                                 │
        ┌────────────────────────────────────────┼────────────────────────────────────────┐
        │                                        │                                        │
        ▼                                        ▼                                        ▼
┌───────────────────┐                 ┌─────────────────────────┐             ┌───────────────────────┐
│   InitializeDoFn  │                 │ DetectNewPartitionsDoFn │             │ReadChangeStreamPartit-│
│                   │                 │         (SDF)           │             │      ionDoFn (SDF)    │
│ - メタデータテー   │                 │                         │             │                       │
│   ブル作成        │                 │ - CREATED状態の         │             │ - Change Stream       │
│ - 初期パーティ    │                 │   パーティション検出    │             │   クエリ実行          │
│   ション挿入      │                 │ - SCHEDULED状態へ       │             │ - レコード処理        │
│                   │                 │   更新                  │             │ - 子パーティション    │
└───────────────────┘                 │ - パーティション出力    │             │   検出                │
                                      └─────────────────────────┘             └───────────────────────┘
```

### 2.2 パーティション状態遷移

```
CREATED ──────► SCHEDULED ──────► RUNNING ──────► FINISHED
   │                │                 │               │
   │                │                 │               │
createdAt      scheduledAt        runningAt      finishedAt
```

### 2.3 データフロー詳細

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│ Phase 1: 初期化                                                                         │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                         │
│  Impulse ──► InitializeDoFn                                                            │
│                   │                                                                     │
│                   ├── メタデータテーブル存在確認                                          │
│                   ├── テーブル未存在時は作成                                              │
│                   └── 初期パーティション挿入 (CREATED状態, token = null)                 │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│ Phase 2: パーティション検出 (継続的ループ)                                               │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                         │
│  DetectNewPartitionsDoFn (SDF)                                                         │
│       │                                                                                 │
│       ├── WatermarkCache から未完了パーティションの最小ウォーターマーク取得              │
│       ├── CREATED状態のパーティションを全て取得                                          │
│       ├── createdAt でグループ化（バッチ処理）                                           │
│       ├── 状態を SCHEDULED に更新                                                       │
│       └── 各パーティションを出力 (minWatermark をタイムスタンプとして)                  │
│                                                                                         │
│  制限追跡:                                                                              │
│       └── TimestampRange [startAt, endAt) をトラッキング                               │
│           tryClaim(createdAt) で進行管理                                               │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│ Phase 3: Change Stream 読み取り (パーティションごと)                                     │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                         │
│  ReadChangeStreamPartitionDoFn (SDF)                                                   │
│       │                                                                                 │
│       ├── 状態を RUNNING に更新                                                         │
│       │                                                                                 │
│       ├── Change Stream クエリ実行:                                                     │
│       │     SELECT * FROM READ_ChangeStreamName(                                       │
│       │       start_timestamp => startTimestamp,                                       │
│       │       end_timestamp => endTimestamp,                                           │
│       │       partition_token => partitionToken,                                       │
│       │       heartbeat_milliseconds => 2000                                           │
│       │     )                                                                          │
│       │                                                                                 │
│       └── 各レコードタイプの処理:                                                       │
│                                                                                         │
│           ┌─────────────────────────────────────────────────────────────────────────┐   │
│           │ DataChangeRecord                                                        │   │
│           │   - commitTimestamp をクレーム                                          │   │
│           │   - ウォーターマーク更新                                                 │   │
│           │   - レコードを出力                                                       │   │
│           └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                         │
│           ┌─────────────────────────────────────────────────────────────────────────┐   │
│           │ ChildPartitionsRecord (パーティション分割/マージ)                        │   │
│           │   - startTimestamp をクレーム                                           │   │
│           │   - 各子パーティション:                                                  │   │
│           │       ├── 分割: parentTokens.size() == 1                               │   │
│           │       └── マージ: parentTokens.size() > 1                              │   │
│           │   - メタデータテーブルに挿入 (CREATED状態)                               │   │
│           │   - 次サイクルで DetectNewPartitions が検出                             │   │
│           └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                         │
│           ┌─────────────────────────────────────────────────────────────────────────┐   │
│           │ HeartbeatRecord                                                         │   │
│           │   - timestamp をクレーム                                                │   │
│           │   - ウォーターマーク更新のみ（出力なし）                                  │   │
│           └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                         │
│       クエリ終了時:                                                                     │
│           ├── 状態を FINISHED に更新                                                   │
│           └── ProcessContinuation.stop()                                               │
│                                                                                         │
│       ソフトタイムアウト発動時:                                                         │
│           └── ProcessContinuation.resume() で再スケジュール                            │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

### 2.4 パーティション分割とマージ

```
パーティション分割 (Split):
──────────────────────────
    Partition A
         │
         ▼
  ChildPartitionsRecord
  (parentTokens: [A])
         │
    ┌────┴────┐
    ▼         ▼
Partition   Partition
   A1          A2


パーティションマージ (Merge):
──────────────────────────
Partition   Partition
   A           B
    │         │
    └────┬────┘
         ▼
  ChildPartitionsRecord
  (parentTokens: [A, B])
         │
         ▼
    Partition C

※マージの場合、複数の親パーティションが同じ子を報告
  → トランザクションで重複挿入を回避
```

### 2.5 ウォーターマーク管理

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                              AsyncWatermarkCache                                        │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                         │
│  ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│  │ LoadingCache (周期的リフレッシュ: デフォルト 100ms)                             │   │
│  │                                                                                 │   │
│  │   getUnfinishedMinWatermark():                                                  │   │
│  │     └── FINISHED以外のパーティションの最小ウォーターマークを計算               │   │
│  │                                                                                 │   │
│  └─────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                         │
│  グローバルウォーターマーク = min(各未完了パーティションのウォーターマーク)             │
│                                                                                         │
│  ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│  │ 各 ReadChangeStreamPartitionDoFn:                                               │   │
│  │   ManualWatermarkEstimator.setWatermark(recordTimestamp)                        │   │
│  │   で明示的にウォーターマークを更新                                               │   │
│  └─────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

### 2.6 BundleFinalizer による At-Least-Once 保証

Apache Beam は **BundleFinalizer** を使用して at-least-once 配信を実現している。
これは Change Stream のようなストリーミング処理において、レコードの欠損を防ぐための重要な仕組みである。

#### 2.6.1 BundleFinalizer の役割

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                           BundleFinalizer のデータフロー                                 │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                         │
│  1. レコード処理                                                                         │
│     DataChangeRecordAction.run():                                                       │
│       ├── tracker.tryClaim(commitTimestamp)  // タイムスタンプをクレーム                │
│       ├── outputReceiver.outputWithTimestamp(record, commitInstant)  // 出力           │
│       └── watermarkEstimator.setWatermark(commitInstant)  // ウォーターマーク更新       │
│                                                                                         │
│  2. BundleFinalizer にコールバック登録                                                   │
│     QueryChangeStreamAction.run():                                                      │
│       └── bundleFinalizer.afterBundleCommit(timeout, updateWatermarkCallback)          │
│                                                                                         │
│  3. Beam Runner によるバンドルコミット                                                   │
│       └── ダウンストリームへの出力を永続化                                               │
│                                                                                         │
│  4. コミット成功後にコールバック実行                                                     │
│       └── partitionMetadataDao.updateWatermark(token, watermark)                       │
│           ↑                                                                             │
│           この時点で初めてウォーターマークが永続化される                                 │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

#### 2.6.2 At-Least-Once が保証される仕組み

```
正常処理:
──────────
[レコード処理] → [出力] → [バンドルコミット成功] → [ウォーターマーク更新]
                                                    ↑
                                          処理完了として永続化

処理中にクラッシュ:
──────────────────
[レコード処理] → [クラッシュ]
                   ↑
     ウォーターマーク未更新 → 再起動時に同じレコードから再処理 ✓

出力後、コミット前にクラッシュ:
──────────────────────────────
[レコード処理] → [出力] → [クラッシュ]
                           ↑
          ウォーターマーク未更新 → 再起動時に同じレコードから再処理 ✓

コミット後、ウォーターマーク更新前にクラッシュ:
──────────────────────────────────────────────
[レコード処理] → [出力] → [バンドルコミット成功] → [クラッシュ]
                                                    ↑
                    ウォーターマーク未更新 → 再起動時に同じレコードから再処理
                    ただしダウンストリームには既に配信済み = 重複発生の可能性
```

#### 2.6.3 重要な注意点

1. **At-Least-Once のみ保証**: Exactly-Once が必要な場合は、ダウンストリームで重複排除が必要
2. **コールバックはベストエフォート**: ファイナライゼーションが失敗しても外部システムは自己回復が期待される
3. **タイムアウト**: デフォルト5分のタイムアウトが設定されている（`BUNDLE_FINALIZER_TIMEOUT`）

#### 2.6.4 関連コード

```java
// QueryChangeStreamAction.java より抜粋
private static final Duration BUNDLE_FINALIZER_TIMEOUT = Duration.standardMinutes(5);

// レコード処理後にコールバックを登録
if (maybeContinuation.isPresent()) {
    bundleFinalizer.afterBundleCommit(
        Instant.now().plus(BUNDLE_FINALIZER_TIMEOUT),
        updateWatermarkCallback(token, watermarkEstimator));
    return maybeContinuation.get();
}

// ウォーターマーク更新コールバック
private BundleFinalizer.Callback updateWatermarkCallback(
    String token, WatermarkEstimator<Instant> watermarkEstimator) {
    return () -> {
        final Instant watermark = watermarkEstimator.currentWatermark();
        partitionMetadataDao.updateWatermark(
            token, Timestamp.ofTimeMicroseconds(watermark.getMillis() * 1_000L));
    };
}
```

### 2.7 主要コンポーネント一覧

| レイヤー | コンポーネント | 責務 |
|---------|---------------|------|
| **DoFn** | InitializeDoFn | 初期化、メタデータテーブル作成 |
| | DetectNewPartitionsDoFn | パーティション検出・スケジューリング |
| | ReadChangeStreamPartitionDoFn | Change Stream クエリ実行 |
| **Action** | QueryChangeStreamAction | クエリ実行とレコードディスパッチ |
| | DataChangeRecordAction | データ変更レコード処理 |
| | ChildPartitionsRecordAction | 子パーティション処理 |
| | HeartbeatRecordAction | ハートビート処理 |
| **DAO** | ChangeStreamDao | Change Stream クエリ実行 |
| | PartitionMetadataDao | メタデータ読み書き |
| **Restriction** | TimestampRangeTracker | タイムスタンプ範囲追跡 |
| | RestrictionInterrupter | ソフトタイムアウト管理 |
| **Cache** | AsyncWatermarkCache | ウォーターマーク非同期キャッシュ |

---

## 3. Go での実装アーキテクチャ

### 3.1 設計原則

単一プロセスで goroutine と channel を活用し、Apache Beam の分散処理モデルを模倣する。

1. **goroutine**: パーティションごとの並列処理を実現
2. **channel**: コンポーネント間のデータ伝播と同期
3. **context.Context**: キャンセレーション・タイムアウト制御
4. **sync パッケージ**: 状態の排他制御

### 3.2 全体アーキテクチャ

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                     Reader                                              │
│                              (エントリーポイント)                                         │
└───────────────────────────────────────┬─────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                   Coordinator                                           │
│                        (全体制御・パーティション管理)                                     │
│                                                                                         │
│  ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│  │ 責務:                                                                           │   │
│  │   - パーティション検出ループの実行                                                │   │
│  │   - PartitionReader の起動・停止管理                                             │   │
│  │   - グローバルウォーターマーク計算                                                │   │
│  │   - グレースフルシャットダウン                                                    │   │
│  └─────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                         │
│  ┌────────────────────────────────────┐     ┌────────────────────────────────────┐    │
│  │      PartitionScheduler            │     │       WatermarkTracker            │    │
│  │  (新規パーティションのスケジュール)   │     │    (ウォーターマーク追跡)          │    │
│  └────────────────────────────────────┘     └────────────────────────────────────┘    │
└───────────────────────────────────────┬─────────────────────────────────────────────────┘
                                        │
            ┌───────────────────────────┼───────────────────────────┐
            ▼                           ▼                           ▼
┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────────┐
│   PartitionReader   │     │   PartitionReader   │     │   PartitionReader   │
│    (goroutine)      │     │    (goroutine)      │     │    (goroutine)      │
│                     │     │                     │     │                     │
│  Partition Token A  │     │  Partition Token B  │     │  Partition Token C  │
└──────────┬──────────┘     └──────────┬──────────┘     └──────────┬──────────┘
           │                           │                           │
           │ chan DataChangeRecord     │ chan DataChangeRecord     │ chan DataChangeRecord
           │                           │                           │
           └───────────────────────────┼───────────────────────────┘
                                       │
                                       ▼
                           ┌───────────────────────┐
                           │    RecordMerger       │
                           │   (順序保証・マージ)   │
                           └───────────┬───────────┘
                                       │
                                       ▼
                           ┌───────────────────────┐
                           │   Output Channel      │
                           │ <-chan ChangeRecord   │
                           └───────────┬───────────┘
                                       │
                                       ▼
                           ┌───────────────────────┐
                           │   Consumer (User)     │
                           └───────────────────────┘
```

### 3.3 コンポーネント詳細

#### 3.3.1 Reader (エントリーポイント)

```go
type Reader struct {
    spannerClient    *spanner.Client
    changeStreamName string
    startTimestamp   time.Time
    endTimestamp     time.Time  // optional: 指定なしは無限ストリーム

    coordinator      *Coordinator
    metadataStore    MetadataStore

    output           chan ChangeRecord
}

// Start は Change Stream の読み取りを開始する
func (r *Reader) Start(ctx context.Context) (<-chan ChangeRecord, error)

// Stop はグレースフルシャットダウンを実行する
func (r *Reader) Stop() error
```

#### 3.3.2 Coordinator (全体制御)

```go
type Coordinator struct {
    mu sync.RWMutex

    // パーティション管理
    partitions       map[string]*PartitionState  // token -> state
    readers          map[string]*PartitionReader // token -> reader

    // 依存コンポーネント
    scheduler        *PartitionScheduler
    watermarkTracker *WatermarkTracker
    metadataStore    MetadataStore

    // チャネル
    partitionEvents  chan PartitionEvent  // 子パーティション検出通知
    recordOutput     chan ChangeRecord

    // 制御
    ctx              context.Context
    cancel           context.CancelFunc
    wg               sync.WaitGroup
}

// PartitionState はパーティションの現在の状態を表す
type PartitionState struct {
    Token           string
    ParentTokens    []string
    StartTimestamp  time.Time
    EndTimestamp    time.Time
    State           State  // CREATED, SCHEDULED, RUNNING, FINISHED
    Watermark       time.Time
    CreatedAt       time.Time
    ScheduledAt     time.Time
    RunningAt       time.Time
    FinishedAt      time.Time
}

type State int

const (
    StateCreated State = iota
    StateScheduled
    StateRunning
    StateFinished
)
```

**Coordinator のメインループ**:

```go
func (c *Coordinator) run(ctx context.Context) {
    // 1. 初期化
    c.initialize(ctx)

    // 2. パーティション検出ループ (goroutine)
    c.wg.Add(1)
    go c.detectNewPartitionsLoop(ctx)

    // 3. パーティションイベント処理ループ
    c.wg.Add(1)
    go c.handlePartitionEvents(ctx)

    // 4. ウォーターマーク更新ループ
    c.wg.Add(1)
    go c.watermarkUpdateLoop(ctx)
}

func (c *Coordinator) detectNewPartitionsLoop(ctx context.Context) {
    defer c.wg.Done()

    ticker := time.NewTicker(100 * time.Millisecond)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            c.detectAndSchedulePartitions(ctx)
        }
    }
}

func (c *Coordinator) detectAndSchedulePartitions(ctx context.Context) {
    c.mu.Lock()
    defer c.mu.Unlock()

    // CREATED状態のパーティションを取得
    created := c.getPartitionsByState(StateCreated)

    for _, p := range created {
        // 状態を SCHEDULED に更新
        p.State = StateScheduled
        p.ScheduledAt = time.Now()

        // PartitionReader を起動
        c.startPartitionReader(ctx, p)
    }
}
```

#### 3.3.3 PartitionReader (パーティション読み取り)

```go
type PartitionReader struct {
    partition        *PartitionState
    spannerClient    *spanner.Client
    changeStreamName string

    // 出力チャネル
    records          chan<- ChangeRecord
    events           chan<- PartitionEvent

    // 制御
    ctx              context.Context
    cancel           context.CancelFunc
}

// PartitionEvent はパーティションイベントを表す
type PartitionEvent struct {
    Type             PartitionEventType
    ChildPartitions  []ChildPartition
    SourcePartition  string
}

type PartitionEventType int

const (
    EventChildPartitions PartitionEventType = iota  // 子パーティション検出
    EventPartitionEnd                                // パーティション終了
)

// ChildPartition は子パーティション情報
type ChildPartition struct {
    Token        string
    ParentTokens []string
    StartTime    time.Time
}
```

**PartitionReader のメインループ**:

```go
func (r *PartitionReader) run(ctx context.Context) error {
    // 状態を RUNNING に更新 (Coordinator経由)
    r.notifyRunning()

    // Change Stream クエリ実行
    stmt := spanner.Statement{
        SQL: `SELECT * FROM READ_@ChangeStreamName(
            start_timestamp => @startTimestamp,
            end_timestamp => @endTimestamp,
            partition_token => @partitionToken,
            heartbeat_milliseconds => 2000
        )`,
        Params: map[string]interface{}{
            "ChangeStreamName": r.changeStreamName,
            "startTimestamp":   r.partition.StartTimestamp,
            "endTimestamp":     r.partition.EndTimestamp,
            "partitionToken":   r.partition.Token,
        },
    }

    iter := r.spannerClient.Single().Query(ctx, stmt)
    defer iter.Stop()

    for {
        row, err := iter.Next()
        if err == iterator.Done {
            break
        }
        if err != nil {
            return r.handleError(err)
        }

        record, err := r.parseRecord(row)
        if err != nil {
            return err
        }

        if err := r.processRecord(ctx, record); err != nil {
            return err
        }
    }

    // パーティション終了通知
    r.events <- PartitionEvent{
        Type:            EventPartitionEnd,
        SourcePartition: r.partition.Token,
    }

    return nil
}

func (r *PartitionReader) processRecord(ctx context.Context, record ChangeStreamRecord) error {
    switch rec := record.(type) {
    case *DataChangeRecord:
        // データ変更レコードを出力
        select {
        case r.records <- rec:
        case <-ctx.Done():
            return ctx.Err()
        }

    case *ChildPartitionsRecord:
        // 子パーティション検出イベントを送信
        r.events <- PartitionEvent{
            Type:            EventChildPartitions,
            ChildPartitions: rec.ChildPartitions,
            SourcePartition: r.partition.Token,
        }

    case *HeartbeatRecord:
        // ウォーターマーク更新のみ (出力なし)
        r.updateWatermark(rec.Timestamp)
    }

    return nil
}
```

#### 3.3.4 PartitionScheduler (スケジューラ)

```go
type PartitionScheduler struct {
    mu sync.Mutex

    // 保留中のパーティション (マージ待ち)
    pendingMerge map[string]*PendingMerge
}

type PendingMerge struct {
    ChildPartition ChildPartition
    ReceivedFrom   map[string]bool  // 受信済み親パーティション
    AllParents     []string
}

// Schedule は新しいパーティションをスケジュールする
// マージの場合、全ての親から報告を受けるまで待機
func (s *PartitionScheduler) Schedule(child ChildPartition, fromParent string) *PartitionState {
    s.mu.Lock()
    defer s.mu.Unlock()

    // 分割の場合 (親が1つ)
    if len(child.ParentTokens) == 1 {
        return s.createPartitionState(child)
    }

    // マージの場合 (親が複数)
    key := child.Token
    pending, exists := s.pendingMerge[key]
    if !exists {
        pending = &PendingMerge{
            ChildPartition: child,
            ReceivedFrom:   make(map[string]bool),
            AllParents:     child.ParentTokens,
        }
        s.pendingMerge[key] = pending
    }

    pending.ReceivedFrom[fromParent] = true

    // 全ての親から受信したかチェック
    if len(pending.ReceivedFrom) == len(pending.AllParents) {
        delete(s.pendingMerge, key)
        return s.createPartitionState(child)
    }

    return nil  // まだ待機中
}
```

#### 3.3.5 WatermarkTracker (ウォーターマーク追跡)

```go
type WatermarkTracker struct {
    mu sync.RWMutex

    // パーティションごとのウォーターマーク
    watermarks map[string]time.Time

    // グローバルウォーターマーク (最小値)
    globalWatermark time.Time
}

// Update は特定パーティションのウォーターマークを更新
func (t *WatermarkTracker) Update(token string, watermark time.Time) {
    t.mu.Lock()
    defer t.mu.Unlock()

    t.watermarks[token] = watermark
    t.recalculateGlobal()
}

// GetGlobal は現在のグローバルウォーターマークを返す
func (t *WatermarkTracker) GetGlobal() time.Time {
    t.mu.RLock()
    defer t.mu.RUnlock()

    return t.globalWatermark
}

func (t *WatermarkTracker) recalculateGlobal() {
    var minWatermark time.Time
    first := true

    for _, wm := range t.watermarks {
        if first || wm.Before(minWatermark) {
            minWatermark = wm
            first = false
        }
    }

    t.globalWatermark = minWatermark
}

// Remove はパーティション終了時にウォーターマークを削除
func (t *WatermarkTracker) Remove(token string) {
    t.mu.Lock()
    defer t.mu.Unlock()

    delete(t.watermarks, token)
    t.recalculateGlobal()
}
```

#### 3.3.6 MetadataStore (メタデータ永続化)

```go
// MetadataStore はパーティションメタデータの永続化インターフェース
type MetadataStore interface {
    // Initialize はメタデータストアを初期化
    Initialize(ctx context.Context) error

    // GetByState は指定状態のパーティションを取得
    GetByState(ctx context.Context, state State) ([]*PartitionState, error)

    // Insert は新しいパーティションを挿入
    Insert(ctx context.Context, partition *PartitionState) error

    // UpdateState はパーティション状態を更新
    UpdateState(ctx context.Context, token string, state State) error

    // UpdateWatermark はウォーターマークを更新
    UpdateWatermark(ctx context.Context, token string, watermark time.Time) error

    // Cleanup はメタデータを削除
    Cleanup(ctx context.Context) error
}

// InMemoryMetadataStore はテスト用のインメモリ実装
type InMemoryMetadataStore struct {
    mu         sync.RWMutex
    partitions map[string]*PartitionState
}

// SpannerMetadataStore は Spanner を使用した実装
// (Apache Beam と同様にメタデータテーブルを使用)
type SpannerMetadataStore struct {
    client    *spanner.Client
    tableName string
}
```

### 3.4 データ型定義

```go
// ChangeRecord は Change Stream から読み取られるレコードの共通インターフェース
type ChangeRecord interface {
    GetCommitTimestamp() time.Time
    GetRecordSequence() string
}

// DataChangeRecord はデータ変更レコード
type DataChangeRecord struct {
    CommitTimestamp        time.Time
    RecordSequence         string
    ServerTransactionID    string
    IsLastRecordInTx       bool
    TableName              string
    ColumnTypes            []ColumnType
    Mods                   []Mod
    ModType                ModType  // INSERT, UPDATE, DELETE
    ValueCaptureType       string
    NumberOfRecordsInTx    int64
    NumberOfPartitionsInTx int64
}

type ModType string

const (
    ModTypeInsert ModType = "INSERT"
    ModTypeUpdate ModType = "UPDATE"
    ModTypeDelete ModType = "DELETE"
)

type Mod struct {
    Keys      map[string]interface{}
    NewValues map[string]interface{}
    OldValues map[string]interface{}
}

// ChildPartitionsRecord はパーティション分割/マージレコード
type ChildPartitionsRecord struct {
    StartTimestamp  time.Time
    RecordSequence  string
    ChildPartitions []ChildPartition
}

// HeartbeatRecord はハートビートレコード
type HeartbeatRecord struct {
    Timestamp time.Time
}
```

### 3.5 Channel 設計

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                               Channel Architecture                                      │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                         │
│  ┌───────────────────────────────────────────────────────────────────────────────────┐ │
│  │ recordChan (buffered: 1000)                                                       │ │
│  │   - PartitionReader → RecordMerger                                                │ │
│  │   - DataChangeRecord の伝播                                                       │ │
│  │   - バッファサイズは背圧制御のため調整可能                                          │ │
│  └───────────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                         │
│  ┌───────────────────────────────────────────────────────────────────────────────────┐ │
│  │ partitionEventChan (buffered: 100)                                                │ │
│  │   - PartitionReader → Coordinator                                                 │ │
│  │   - ChildPartitionsRecord, パーティション終了イベント                              │ │
│  └───────────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                         │
│  ┌───────────────────────────────────────────────────────────────────────────────────┐ │
│  │ outputChan (unbuffered or buffered: depends on consumer)                          │ │
│  │   - RecordMerger → Consumer                                                       │ │
│  │   - 最終的な DataChangeRecord 出力                                                │ │
│  └───────────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                         │
│  ┌───────────────────────────────────────────────────────────────────────────────────┐ │
│  │ controlChan (各 PartitionReader)                                                  │ │
│  │   - Coordinator → PartitionReader                                                 │ │
│  │   - 停止・一時停止などの制御シグナル                                               │ │
│  └───────────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

### 3.6 エラーハンドリングとリトライ

```go
// RetryConfig はリトライ設定
type RetryConfig struct {
    MaxRetries     int
    InitialBackoff time.Duration
    MaxBackoff     time.Duration
    Multiplier     float64
    RetryableCodes []codes.Code
}

var DefaultRetryConfig = RetryConfig{
    MaxRetries:     5,
    InitialBackoff: 100 * time.Millisecond,
    MaxBackoff:     30 * time.Second,
    Multiplier:     2.0,
    RetryableCodes: []codes.Code{
        codes.Unavailable,
        codes.Aborted,
        codes.DeadlineExceeded,
    },
}

// withRetry はリトライロジックをラップする
func withRetry[T any](ctx context.Context, config RetryConfig, op func() (T, error)) (T, error) {
    var result T
    var lastErr error

    backoff := config.InitialBackoff

    for attempt := 0; attempt <= config.MaxRetries; attempt++ {
        result, lastErr = op()
        if lastErr == nil {
            return result, nil
        }

        // リトライ可能かチェック
        if !isRetryable(lastErr, config.RetryableCodes) {
            return result, lastErr
        }

        // バックオフ待機
        select {
        case <-ctx.Done():
            return result, ctx.Err()
        case <-time.After(backoff):
        }

        backoff = time.Duration(float64(backoff) * config.Multiplier)
        if backoff > config.MaxBackoff {
            backoff = config.MaxBackoff
        }
    }

    return result, fmt.Errorf("max retries exceeded: %w", lastErr)
}
```

### 3.7 グレースフルシャットダウン

```go
func (c *Coordinator) Shutdown(ctx context.Context) error {
    // 1. 新規パーティションのスケジューリング停止
    c.cancel()

    // 2. 全 PartitionReader に停止シグナル
    c.mu.Lock()
    for _, reader := range c.readers {
        reader.cancel()
    }
    c.mu.Unlock()

    // 3. 全 goroutine の終了を待機
    done := make(chan struct{})
    go func() {
        c.wg.Wait()
        close(done)
    }()

    select {
    case <-done:
        // 正常終了
    case <-ctx.Done():
        return ctx.Err()
    }

    // 4. 残りのレコードをフラッシュ
    close(c.recordOutput)

    // 5. メタデータのクリーンアップ (オプション)
    // c.metadataStore.Cleanup(ctx)

    return nil
}
```

### 3.8 使用例

```go
func main() {
    ctx := context.Background()

    // Spanner クライアント作成
    client, err := spanner.NewClient(ctx, "projects/my-project/instances/my-instance/databases/my-db")
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // Reader 作成
    reader := NewReader(ReaderConfig{
        SpannerClient:    client,
        ChangeStreamName: "MyChangeStream",
        StartTimestamp:   time.Now().Add(-1 * time.Hour),
        // EndTimestamp:  指定なしで無限ストリーム
    })

    // 読み取り開始
    records, err := reader.Start(ctx)
    if err != nil {
        log.Fatal(err)
    }

    // シグナルハンドリング
    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

    // レコード処理
    for {
        select {
        case record, ok := <-records:
            if !ok {
                log.Println("Stream closed")
                return
            }

            // レコード処理
            processRecord(record)

        case <-sigCh:
            log.Println("Shutting down...")

            shutdownCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
            defer cancel()

            if err := reader.Stop(shutdownCtx); err != nil {
                log.Printf("Shutdown error: %v", err)
            }
            return
        }
    }
}

func processRecord(record ChangeRecord) {
    switch r := record.(type) {
    case *DataChangeRecord:
        fmt.Printf("[%s] %s.%s: %v\n",
            r.ModType, r.TableName, r.Mods[0].Keys, r.Mods)
    }
}
```

### 3.9 Apache Beam との対応関係

| Apache Beam | Go 実装 |
|-------------|---------|
| InitializeDoFn | Reader.initialize() |
| DetectNewPartitionsDoFn | Coordinator.detectNewPartitionsLoop() |
| ReadChangeStreamPartitionDoFn | PartitionReader.run() |
| TimestampRangeTracker | Coordinator による状態管理 |
| AsyncWatermarkCache | WatermarkTracker |
| PartitionMetadataDao | MetadataStore interface |
| QueryChangeStreamAction | PartitionReader.processRecord() |
| ChildPartitionsRecordAction | PartitionScheduler.Schedule() |
| ProcessContinuation.resume() | goroutine の再起動 |
| ParDo の並列処理 | 複数 goroutine |
| PCollection の伝播 | channel による伝播 |

### 3.10 考慮事項と制限

#### 利点
- 単一プロセスでシンプルな運用
- goroutine による効率的な並列処理
- channel による明確なデータフロー

#### 制限
- 単一プロセスのためスケールアウトが困難
- プロセス障害時の復旧にはチェックポイント永続化が必要
- 大量のパーティションがある場合、goroutine 数が増大

#### 将来の拡張
1. **分散対応**: メタデータストアを共有し、複数プロセスでパーティションを分担
2. **チェックポイント**: 定期的な状態永続化による障害復旧
3. **バックプレッシャー**: channel バッファとレート制限による流量制御
4. **メトリクス**: Prometheus 連携によるモニタリング

---

## 参考資料

- [Apache Beam Programming Guide](https://beam.apache.org/documentation/programming-guide/)
- [Splittable DoFn](https://beam.apache.org/documentation/programming-guide/#splittable-dofns)
- [Cloud Spanner Change Streams](https://cloud.google.com/spanner/docs/change-streams)
- [Cloud Spanner Change Stream Partitions](https://cloud.google.com/spanner/docs/change-streams/partitions)
