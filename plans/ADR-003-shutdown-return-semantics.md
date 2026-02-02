# ADR-003: 終了時の戻り値セマンティクスの整理

## ステータス

承認済み

## 要約

`Subscribe()` の終了時戻り値を整理し、正常終了（全パーティション完了・Shutdown）は `nil` を、強制終了（Close）は `ErrSubscriberClosed` を返すように変更する。

## コンテキスト

### 現状の実装

coordinator には複数の終了パスが存在する:

```go
func (c *coordinator) run() error {
    // ...
    if c.err != nil {
        return c.err
    }
    // Shutdown/Close による終了の場合
    if c.ctx.Err() != nil {
        return ErrSubscriberClosed
    }
    return nil
}

func (c *coordinator) initiateShutdown() {
    c.cancel(nil)  // 全パーティション完了時
}

func (c *coordinator) shutdown(ctx context.Context) error {
    c.cancel(nil)  // 外部からの graceful shutdown
    // ...
}

func (c *coordinator) close() error {
    c.cancel(ErrSubscriberClosed)  // 強制終了
    // ...
}
```

### 発見された問題

`run()` の終了判定で `c.ctx.Err() != nil` をチェックしているが、これでは `cancel(nil)` と `cancel(ErrSubscriberClosed)` を区別できない。

| トリガー | cancel 引数 | 現状の Subscribe() 戻り値 |
|---------|------------|-------------------------|
| 全パーティション完了 | `nil` | `ErrSubscriberClosed` |
| `Shutdown()` | `nil` | `ErrSubscriberClosed` |
| `Close()` | `ErrSubscriberClosed` | `ErrSubscriberClosed` |
| エラー発生 | そのエラー | そのエラー |

すべての終了パスが同じエラーを返しており、呼び出し側で区別できない。

## 検討した選択肢

### 選択肢 A: http.Server パターンに倣う

`http.Server` では:
- `Serve()` / `ListenAndServe()` は Shutdown/Close 後に必ず `ErrServerClosed` を返す
- `Shutdown()` 自体は成功時 `nil` を返す

**メリット:**
- 標準ライブラリと一貫したセマンティクス

**デメリット:**
- 「全パーティション完了」という正常終了も `ErrSubscriberClosed` になる
- Go の慣習（正常終了 = `nil`）に反する

### 選択肢 B: 正常終了は nil、強制終了は ErrSubscriberClosed

- 全パーティション完了 → `nil`
- `Shutdown()` → `nil`
- `Close()` → `ErrSubscriberClosed`

**メリット:**
- Go の慣習に沿っている
- 呼び出し側のコードが自然になる

**デメリット:**
- `http.Server` とは異なるセマンティクス

## 詳細な議論

### http.Server が常にエラーを返す理由

Go の Issue #11219 を調査した結果、`http.Server.Serve()` が常にエラーを返す設計は **意図的な設計ではなく、歴史的経緯と互換性制約の結果** であることが判明した。

#### 経緯

1. **Go 1 時代（2012年）**: `Serve` は Accept ループで、Listener が閉じられるとエラーで終了する設計だった。当時は graceful shutdown の仕組み自体がなく、「サーバーが止まる = 何か問題が起きた」という前提だった。

2. **Issue #11219（2015年）**: 「Serve が Listener close 時にエラーを返すのはおかしい、nil を返すべき」という提案があった。Brad Fitzpatrick の回答は **"The behavior of those functions is frozen."** であり、Go 1 互換性ポリシーにより変更できなかった。

3. **Go 1.8（2017年）**: `Shutdown()` と `ErrServerClosed` が導入されたが、Serve の戻り値セマンティクスは変更できなかった。代わりに sentinel error で「意図的な終了」を識別可能にした。

#### 結論

`http.Server` は互換性制約で変えられなかっただけであり、新規設計の spream がこれに倣う必要はない。実際、一部の開発者は「nil を返すべきだった」と考えている。

> "Some consider this was a mistake on the Go team's side, seeing no reason why returning nil wouldn't have been better and more properly aligned with Go conventions."

### context.Cause による終了理由の識別

Go 1.20 で導入された `context.WithCancelCause` と `context.Cause` を使えば、cancel 時に渡した値で終了理由を識別できる:

```go
c.ctx, c.cancel = context.WithCancelCause(context.Background())

// 終了時
c.cancel(errGracefulShutdown)  // initiateShutdown / shutdown（プライベートセンチネル）
c.cancel(ErrSubscriberClosed)  // close

// run() での判定
if cause := context.Cause(c.ctx); cause != nil && cause != errGracefulShutdown {
    return cause  // close() やエラーの場合
}
return nil  // initiateShutdown() / shutdown() の場合
```

**注**: `cancel(nil)` の場合、`context.Cause()` は `context.Canceled` を返す（Go の仕様）。そのため、正常終了を識別するにはプライベートなセンチネルエラー `errGracefulShutdown` を使用する。

### 呼び出し側のコード比較

**選択肢 A（http.Server パターン）:**

```go
err := subscriber.Subscribe()
if err != nil && !errors.Is(err, spream.ErrSubscriberClosed) {
    log.Fatal(err)
}
// 全パーティション完了も ErrSubscriberClosed なので区別できない
```

**選択肢 B（正常終了は nil）:**

```go
err := subscriber.Subscribe()
if err != nil {
    if errors.Is(err, spream.ErrSubscriberClosed) {
        log.Println("Subscriber was forcibly closed")
    } else {
        log.Fatal(err)
    }
}
// nil なら正常終了（全パーティション完了 or Shutdown）
```

選択肢 B の方が Go の慣習に沿っており、コードの意図が明確になる。

## 決定

**選択肢 B を採用**: 正常終了（全パーティション完了・Shutdown）は `nil` を、強制終了（Close）は `ErrSubscriberClosed` を返す。

| トリガー | Subscribe() 戻り値 |
|---------|-------------------|
| 全パーティション完了 | `nil` |
| `Shutdown()` 成功 | `nil` |
| `Shutdown()` 中断 (timeout/cancel) | `ErrShutdownAborted` |
| `Close()` | `ErrSubscriberClosed` |
| エラー発生 | そのエラー |

**注**: `Shutdown()` がタイムアウトまたはキャンセルされた場合、`Subscribe()` は `ErrShutdownAborted` を返す。これにより呼び出し側は in-flight 操作が完了しなかったことを検知できる。

## 理由

1. **Go の慣習に従う**: 正常終了は `nil` を返すのが Go の標準的なパターン
2. **新規設計の自由度**: spream は新規ライブラリであり、`http.Server` の互換性制約に縛られる必要がない
3. **意図の明確さ**: `ErrSubscriberClosed` は「強制終了された」という明確な意味を持つ
4. **呼び出し側の簡潔さ**: `err != nil` だけで異常系を判定できる

## 結果

### 変更するコード

**coordinator.go の run() 終了判定:**

```go
// Before
if c.ctx.Err() != nil {
    return ErrSubscriberClosed
}

// After
if cause := context.Cause(c.ctx); cause != nil && cause != errGracefulShutdown {
    return cause
}
return nil
```

### 影響

- `Subscribe()` が `nil` を返すケースが増える（全パーティション完了、Shutdown 成功時）
- `ErrSubscriberClosed` は `Close()` による強制終了時のみ返される
- 呼び出し側のエラーハンドリングコードがシンプルになる

### ドキュメントへの記載事項

`Subscribe()` の godoc に以下を明記する:

```go
// Subscribe starts subscribing to the change stream.
// It blocks until one of the following occurs:
//   - All partitions are processed (returns nil)
//   - Shutdown is called (returns nil)
//   - Close is called (returns ErrSubscriberClosed)
//   - An error occurs (returns the error)
```

## 参考資料

- [Go Issue #11219: Server.Serve returns an error when the Listener is closed](https://github.com/golang/go/issues/11219)
- [Go Issue #26267: imprecise description of the return values of HTTP serve functions](https://github.com/golang/go/issues/26267)
- [SoByte: Go http server shutdown done right](https://www.sobyte.net/post/2021-10/go-http-server-shudown-done-right/)
- [Go 標準ライブラリ net/http/server.go](https://cs.opensource.google/go/go/+/refs/tags/go1.22.0:src/net/http/server.go)
