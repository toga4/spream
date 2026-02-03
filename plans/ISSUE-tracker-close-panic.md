# Issue: tracker.close() 後に complete() が呼ばれると panic する

## 概要

`inflightTracker.close()` が呼ばれた後に `complete()` が呼ばれると、closed チャネルへの送信で panic が発生する。

## 再現シナリオ

1. `Consumer.Consume()` が goroutine で実行中
2. `coordinator.close()` が呼ばれる
3. `reader.Close()` → `tracker.close()` で `watermarks` と `errors` チャネルが closed
4. `Consumer.Consume()` が完了し、`tracker.complete()` が呼ばれる
5. **panic: send on closed channel**

## 問題のコード

```go
// inflight_tracker.go

func (t *inflightTracker) close() {
    if t.closed.Swap(true) {
        return
    }
    close(t.watermarks)  // チャネルを close
    close(t.errors)      // チャネルを close
    t.closeDone()
}

func (t *inflightTracker) complete(seq int64, err error) {
    // ... 処理 ...

    // close 後にここに到達すると panic
    if err != nil {
        t.errors <- err
    } else if !newWatermark.IsZero() {
        t.watermarks <- newWatermark
    }
}
```

## 修正案

### 案1: complete() で closed フラグをチェック（race condition あり）

```go
func (t *inflightTracker) complete(seq int64, err error) {
    t.mu.Lock()

    if t.closed.Load() {
        t.sem.Release(1)
        t.mu.Unlock()
        return
    }

    // ... 処理 ...
}
```

問題: `t.closed.Load()` とチャネル送信の間に `close()` が呼ばれる可能性がある。

### 案2: mutex でチャネル操作を保護

```go
func (t *inflightTracker) close() {
    if t.closed.Swap(true) {
        return
    }

    t.mu.Lock()
    close(t.watermarks)
    close(t.errors)
    t.mu.Unlock()

    t.closeDone()
}

func (t *inflightTracker) complete(seq int64, err error) {
    t.mu.Lock()
    defer t.mu.Unlock()

    if t.closed.Load() {
        t.sem.Release(1)
        return
    }

    // ... 処理 ...

    t.sem.Release(1)

    // mutex 内でチャネル送信
    if err != nil {
        t.errors <- err
    } else if !newWatermark.IsZero() {
        t.watermarks <- newWatermark
    }
}
```

問題: チャネル送信を mutex 内で行うと、受信側がブロックしている場合にデッドロックの可能性がある。

### 案3: select で非ブロッキング送信

```go
func (t *inflightTracker) complete(seq int64, err error) {
    t.mu.Lock()

    if t.closed.Load() {
        t.sem.Release(1)
        t.mu.Unlock()
        return
    }

    var newWatermark time.Time
    if err != nil {
        // エラー時は ack しない
    } else {
        if rec, ok := t.pending[seq]; ok {
            rec.acked = true
        }
        newWatermark = t.advanceWatermark()
    }

    t.sem.Release(1)
    t.mu.Unlock()

    // 非ブロッキング送信（バッファが満杯 or closed なら無視）
    if err != nil {
        select {
        case t.errors <- err:
        default:
        }
    } else if !newWatermark.IsZero() {
        select {
        case t.watermarks <- newWatermark:
        default:
        }
    }
}
```

問題: エラーやウォーターマークが失われる可能性がある。ただし close 後なので許容できるかもしれない。

### 案4: closed フラグのチェックを送信直前に再度行う

```go
func (t *inflightTracker) complete(seq int64, err error) {
    t.mu.Lock()

    if t.closed.Load() {
        t.sem.Release(1)
        t.mu.Unlock()
        return
    }

    var newWatermark time.Time
    var sendErr error

    if err != nil {
        sendErr = err
    } else {
        if rec, ok := t.pending[seq]; ok {
            rec.acked = true
        }
        newWatermark = t.advanceWatermark()
    }

    t.sem.Release(1)
    t.mu.Unlock()

    // 送信直前に再チェック（完全ではないが race window を小さくする）
    if t.closed.Load() {
        return
    }

    if sendErr != nil {
        t.errors <- sendErr
    } else if !newWatermark.IsZero() {
        t.watermarks <- newWatermark
    }
}
```

## 推奨

**案3（非ブロッキング送信）** を推奨する。

理由:
- close 後のエラーやウォーターマークは無視しても問題ない（強制終了なのでデータロスは許容）
- デッドロックのリスクがない
- 実装がシンプル

ただし、チャネルのバッファサイズを適切に設定する必要がある（現在は `maxInflight` サイズ）。

## 関連

- ADR-003: 終了時の戻り値セマンティクスの整理
- ADR_GRACEFUL_SHUTDOWN.md
