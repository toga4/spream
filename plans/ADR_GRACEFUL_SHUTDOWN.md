# ADR: Graceful Shutdown API 設計

## ステータス

Accepted

## 日付

2026-02-01

## コンテキスト

spream は Spanner Change Stream を購読するライブラリである。長時間動作する worker process として使用されることを想定している。

### 発端となった問題

coordinator のエラーハンドリングを調査する中で、以下の問題が浮上した:

1. **`initiateShutdown` と `c.cancel` の混同**: 両方が同じ `c.cancel()` を呼ぶが、意図が異なる
2. **graceful shutdown の不在**: `http.Server.Shutdown` 相当のメソッドがない
3. **終了パターンの曖昧さ**: 正常終了、graceful shutdown、強制終了の区別がない

### 調査で判明した事実

- **現状のコードは正しく動作している**: `context.Canceled` の場合は `UpdateToFinished()` は呼ばれず、State は `RUNNING` のまま残り、次回リカバリされる
- **問題は可読性とAPI設計**: コードの意図が読みにくく、終了パターンが明確でない

## 議論の詳細

### 1. 責務の分離

partition_reader が `context.Canceled` を「正常終了」と判断していた:

```go
if errors.Is(err, context.Canceled) {
    return nil  // reader が勝手に「正常」と判断
}
```

これを coordinator に判断を委ねるべきという議論から、`context.WithCancelCause` の導入を検討。

### 2. Shutdown メソッドの必要性

実運用を考えると `http.Server.Shutdown` 相当のメソッドが必要:

- 新しいパーティションのスケジュールを停止
- 処理中（in-flight）のレコードは完了まで待つ（drain）
- タイムアウトを設定可能

### 3. Subscribe に context を渡すべきか

Go issue #52805 で「context-based graceful server shutdown」が提案されたが却下された。

**却下理由（Damien Neil）**:
> context に「サーバーの寿命」と「shutdown のタイムアウト」の2つの意味を持たせるのは混乱を招く

`http.Server.Shutdown(ctx)` の ctx は「shutdown のタイムアウト」であり、`ListenAndServe()` は context を受け取らない。

### 4. spream への適用

spream も長寿命のサービスを前提としている:
- Change Stream は基本的に `endTimestamp` なしで読む（無期限）
- worker process として起動し続ける

これは `http.Server` と同じユースケースパターン。

### 5. drain のタイムアウト

当初、drain に固定タイムアウト（30秒）を設けていたが、以下の理由で削除:
- Shutdown の context がタイムアウトしたら呼び出し側が Close を呼ぶ設計で十分
- タイムアウトを coordinator から partition_reader に伝播させる仕組みが複雑になる

## 決定

`http.Server` と同様のパターンを採用する:

```go
func (s *Subscriber) Subscribe(consumer Consumer) error      // context なし
func (s *Subscriber) Shutdown(ctx context.Context) error     // graceful shutdown
func (s *Subscriber) Close() error                           // 強制終了
```

### 終了パターン

| パターン | トリガー | drain | State |
|---------|---------|-------|-------|
| 正常完了 | endTimestamp 到達 | ✓ | FINISHED |
| graceful shutdown | `Shutdown()` | ✓ | RUNNING |
| 強制終了 | `Close()` | ✗ | RUNNING |

### context.CancelCauseFunc の使用

`context.WithCancelCause` を使って終了理由を伝播:

| 呼び出し元 | cause | drain |
|-----------|-------|-------|
| `initiateShutdown()` | `errGracefulShutdown` | ✓ |
| `shutdown()` | `errGracefulShutdown` | ✓ |
| `close()` | `ErrSubscriberClosed` | ✗ |
| `recordError(err)` | `err` | ✗ |

partition_reader は `context.Cause(ctx)` で判断:
- `cause == errGracefulShutdown` → graceful shutdown → drain する
- それ以外 → 強制終了 → drain しない

## 理由

1. **Go の設計思想に沿う**: Go チームも context で長寿命サービスのライフサイクルを管理することに慎重
2. **意図が明確**: 終了パターンを明示的なメソッドで分離
3. **http.Server と同じパターン**: Go 開発者にとって馴染みやすい
4. **シンプル**: `context.Cause` で drain の有無を判断、複雑なフラグやコールバック不要

## 結果

### API の変更

- `Subscribe(ctx, consumer)` → `Subscribe(consumer)` (破壊的変更)
- `Shutdown(ctx)` 追加
- `Close()` 追加
- `ErrSubscriberClosed` 追加

### 影響

- v0.x なので破壊的変更は許容される（v1.0 は retract 済み）
- 既存コードは `Subscribe` の引数を変更する必要がある

### 今回スコープ外

- `initiateShutdown` の名前の整理
- タイムアウト時のログ出力

## 参考

- [Go net/http source (server.go)](https://github.com/golang/go/blob/master/src/net/http/server.go)
- [Go issue #52805: context based graceful server shutdown](https://github.com/golang/go/issues/52805)
- [Proper HTTP shutdown in Go - DEV Community](https://dev.to/mokiat/proper-http-shutdown-in-go-3fji)
