# ADR-001: events チャネルの削除とイベント処理の簡略化

## ステータス

提案中

## 要約

coordinator と partitionReader 間の通信に使用している events チャネルを削除し、partitionReader の終了処理を直接 goroutine 内で行う。

## コンテキスト

### 現状の実装

現在、partitionReader から coordinator への通知に `events` チャネルを使用している:

```go
type partitionEvent struct {
    eventType       partitionEventType
    partition       *PartitionMetadata
    childPartitions *ChildPartitionsRecord
    err             error
}

const (
    eventChildPartitions partitionEventType = iota
    eventPartitionFinished
    eventPartitionError
)
```

coordinator は `handleEvents()` ループでこれらのイベントを処理する。

### 発見された問題

コードレビュー中に以下の疑問が生じた:

**eventChildPartitions が何も処理していない**

```go
func (c *coordinator) processEvent(event partitionEvent) {
    switch event.eventType {
    case eventChildPartitions:
        // Child partitions have been added to storage, nothing else to do here.
        // The next tick of detectNewPartitionsLoop will pick them up.
    case eventPartitionFinished:
        c.removeReader(event.partition.PartitionToken)
    case eventPartitionError:
        c.removeReader(event.partition.PartitionToken)
        c.recordError(event.err)
    }
}
```

eventChildPartitions のケースでは何も行わず、次のポーリングで子パーティションが検出されるのを待っている。これなら通知自体が不要ではないか？

## 検討した選択肢

### 選択肢 A: 即時スケジューリングの追加

eventChildPartitions を受信したら、即座に子パーティションをスケジュールする。

**メリット:**
- 子パーティション起動のレイテンシ削減
- ポーリング間隔を長くできる

**デメリット:**
- 複雑性の増加
- 終了検出のロジックが複雑になる
- merge で同じパーティションに複数回通知が来た場合の考慮が必要

### 選択肢 B: ポーリングに寄せて events チャネルを削除

eventChildPartitions を削除し、子パーティション検出は detectNewPartitionsLoop のポーリングに一本化する。さらに events チャネル自体を削除する。

**メリット:**
- シンプル
- 終了検出がポーリング内で完結
- goroutine が1つ減る
- イベント順序の考慮が不要

**デメリット:**
- 子パーティション起動がポーリング間隔に依存（デフォルト1秒）

## 詳細な議論

### 即時スケジューリング時の終了検出の問題

即時スケジューリングを導入した場合、「すべての reader が終了した」ことで終了を検出できるか検討した。

**問題**: イベント処理順序の保証

```
Reader A: ChildPartitionsRecord 処理 → eventChildPartitions 送信 → 終了 → eventPartitionFinished 送信
```

同一 goroutine からの送信なので順序は保証されるが、即時スケジューリングが失敗した場合:

1. eventChildPartitions 処理 → スケジューリング失敗
2. eventPartitionFinished 処理 → readers = {}
3. 終了検出が発動
4. しかし子パーティションは storage に CREATED で残っている

結論: **readers が空でも storage のチェックは省略できない**。それなら現在の `GetUnfinishedMinWatermarkPartition() == nil` によるポーリング方式の方がシンプル。

### パーティション merge 時の順序保証について

Google Cloud ドキュメントの調査:

> "To ensure a strict ordered processing of data records for a particular key, the query on child_token_4 must start after all the parents have finished."

厳密な順序保証が必要なら、すべての親パーティションが finished になるまで子パーティションの開始を待つべきと記載されている。

しかし、Apache Beam の実装を調査した結果:
- Beam も明示的な「すべての親が finished」チェックはしていない
- Watermark ベースのスケジューリングで近似している（spream と同様）

**結論**: ドキュメントの記述は「厳密な順序が必要な場合」の指針であり、必須要件ではない。イベントの漏れがなければ、順序は Consumer 側で制御可能。現在の実装で問題ない。

### events チャネル削除の安全性

eventPartitionFinished / eventPartitionError を直接処理に変更:

```go
// Before
go func() {
    defer c.wg.Done()
    if err := reader.run(c.ctx); err != nil {
        c.events <- partitionEvent{eventType: eventPartitionError, ...}
    } else {
        c.events <- partitionEvent{eventType: eventPartitionFinished, ...}
    }
}()

// After
go func() {
    defer c.wg.Done()
    err := reader.run(c.ctx)
    c.removeReader(partition.PartitionToken)
    if err != nil {
        c.recordError(err)
    }
}()
```

**並行性の安全性:**
- `removeReader`: mutex で保護されている
- `recordError`: sync.Once で保護されている

**処理順序:**
- removeReader / recordError は順序非依存
- 複数の goroutine から同時に呼ばれても問題ない

## 決定

**選択肢 B を採用**: events チャネルを削除し、ポーリングベースのアーキテクチャに一本化する。

## 理由

1. **シンプルさ**: コードの複雑性を減らすことが最優先
2. **十分なレイテンシ**: デフォルト1秒のポーリング間隔で実用上問題ない
3. **YAGNI**: 即時スケジューリングは必要になってから追加しても遅くない
4. **終了検出の明確さ**: ポーリング内で `GetUnfinishedMinWatermarkPartition() == nil` をチェックするだけ

## 結果

### 削除するもの

- `partitionEvent` 構造体
- `partitionEventType` 型と定数 (`eventChildPartitions`, `eventPartitionFinished`, `eventPartitionError`)
- `coordinator.events` チャネル
- `coordinator.handleEvents()` メソッド
- `coordinator.processEvent()` メソッド
- `partitionReader.events` フィールド
- `processChildPartitionsRecord` 内の通知処理
- `run()` 内の `go c.handleEvents()` と対応する `c.wg.Add(1)`

### 変更するもの

- `startPartitionReader`: goroutine 内で直接 `removeReader` / `recordError` を呼ぶ
- `newPartitionReader`: events パラメータを削除

### 影響

- goroutine が1つ減少（handleEvents ループ）
- チャネル管理のオーバーヘッド削除
- コードの見通しが良くなる

### ドキュメントへの記載事項

以下の内容を README またはドキュメントに明記する必要がある:

- **順序性の保証がないこと**: 複数のパーティションから返されるレコードの処理順序は保証されない。同一キーに対する変更を commit timestamp 順で処理する必要がある場合、Consumer 側で順序制御を行う必要がある。
- Google Cloud のドキュメントでは「厳密な順序保証が必要な場合、すべての親パーティションが finished になってから子パーティションのクエリを開始すべき」と記載されているが、本ライブラリはこの制約を強制しない設計である。

## 将来の拡張について

本決定により events チャネルを削除するが、将来的に coordinator が管理するコンポーネント（partitionReader 等）からイベント通知を受ける必要が生じた場合、このイベントチャネルアーキテクチャを再度採用することを検討すべきである。

**再採用を検討すべきケース:**

- 子パーティション検出時の即時スケジューリングが必要になった場合
- partitionReader の状態変化を coordinator がリアルタイムに把握する必要が生じた場合
- メトリクス収集やロギングのためにイベントを集約する必要が生じた場合

**再採用時の設計指針:**

- バッファ付きチャネルを使用し、送信側のブロッキングを防ぐ
- イベントタイプを enum で明確に定義する
- handleEvents ループを専用の goroutine で実行する
- イベント処理の順序依存性を最小限にする

## 参考資料

- [Change stream partitions, records, and queries | Google Cloud](https://cloud.google.com/spanner/docs/change-streams/details)
- [Apache Beam SpannerIO ソースコード](https://github.com/apache/beam/tree/master/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/spanner/changestreams)
