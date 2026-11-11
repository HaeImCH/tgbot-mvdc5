# tgbot_mvdc5

把 Telegram 檔案搬到你帳號所在 DC 的 Rust 服務。

轉傳一個檔案給它，如果檔案不在本帳號的 DC（例如在 DC4，而帳號在 DC5），它會用多條連線從原本的 DC 下載，再重新上傳到本帳號的 DC，最後把新檔案傳回給你。整個過程會持續更新一則繁體中文的狀態訊息（階段、進度、下載／上傳速度）。

使用 [grammers](https://codeberg.org/Lonami/grammers) 0.12（MTProto layer 233）。

## 功能

- **兩種帳號模式**
  - `bot`：用 bot token 登入，任何人（或 `allowed_user_ids` 中的人）私訊檔案給 bot 即可。上傳上限 2000 MiB。
  - `user`：用你自己的帳號登入，處理你轉傳到「儲存的訊息」的檔案，以及 `allowed_user_ids` 中的使用者私訊給你的檔案。Premium 帳號上傳上限 4000 MiB。不會回覆任何文字訊息。
- **多連線、管線化傳輸**：每條連線同時保有多個請求（預設 4 條 × 8 個 × 512 KB = 16 MB 在途，與 Telegram Desktop 相同）。
- **媒體專用 DC 位址**：跨 DC 下載走 `help.getConfig` 裡的 `media_only` 位址（Telegram Desktop 的 media cluster 做法），連不上時自動退回一般位址。
- **每條連線獨立授權**：Telegram 的下載限流以授權為單位，獨立授權可避免所有連線一起被限流。
- **穩定性**：請求逾時、無聲卡死偵測（watchdog）、FLOOD_WAIT / FLOOD_PREMIUM_WAIT 處理、斷線重連（含隨機抖動與指數退避）、檔案參照過期時自動重新取得。
- **大檔案分割**：超過帳號上傳上限的檔案會分割成 `.001`、`.002`… 並附上合併指令（`cat` / `copy /b`）。
- **預熱連線池**：預先連線到各 DC 並定期 ping，新任務可立即開始。
- **SOCKS5 代理**：所有連線（含多條下載／上傳連線）都經過代理，支援帳號密碼。
- **同 DC 重新上傳**（可選）：`reupload_same_dc = true` 時，已在本帳號 DC 的檔案也會重新下載並上傳。
- **清理**：啟動時清空暫存資料夾；收到 SIGTERM / Ctrl+C 時刪除未完成的暫存檔，並把狀態訊息改成「已中斷」。
- **完整的記錄**：每個請求都會記錄來源、檔名（經過跳脫）與結果（完成、失敗、略過、中斷）。

## 實測速度

測試檔案：1.93 GB，位於 DC4，帳號在 DC5，伺服器在台北，經 SOCKS5 代理。

| 項目 | 速度 |
|---|---|
| 從 DC4 下載（媒體位址，4 × 8） | 約 64 MB/s，0 次 FLOOD_WAIT |
| 從 DC4 下載（一般位址，改版前） | 約 13 MB/s，約 1500 次 FLOOD_WAIT |
| 從 DC5 下載（同 DC，媒體位址） | 約 104 MB/s |
| 上傳到 DC5 | 約 25–27 MB/s |

已知限制：

- **上傳約 27 MB/s 是 Telegram 端的上限**：增加連線數、同時上傳多個檔案、換帳號、不走代理、改用媒體位址都一樣，伺服器本身的上傳頻寬遠高於此。
- **bot 帳號的下載仍會被限流**（約 14 MB/s，大量 FLOOD_WAIT），即使走媒體位址也一樣。大檔案建議用 `user` 模式。
- **DC3 沒有媒體位址**，從 DC3 下載仍走一般位址。

## 建置

```sh
cargo build --release
cargo test
```

交叉編譯到 Linux amd64（需要 [cross](https://github.com/cross-rs/cross) 與 Docker）：

```sh
CROSS_CONTAINER_OPTS="--platform linux/amd64" \
  cross build --release --target x86_64-unknown-linux-gnu
```

`vendor/grammers-mtsender` 是 grammers-mtsender 0.12.0 加上一個小修改（把 `Sender::enqueue_body` 設為 `pub`，供管線化傳輸使用），透過 `Cargo.toml` 的 `[patch.crates-io]` 套用，詳見 `vendor/grammers-mtsender/PATCH.md`。

## 設定

```sh
cp config.example.toml config.toml
```

至少要填：

- `api_id`、`api_hash`：到 <https://my.telegram.org> → API development tools 申請。
- `bot` 模式：bot token 放在 `token.txt`（或直接填 `bot_token`）。
- `user` 模式：`account = "user"`，並使用與 bot 不同的 `session_file`。

常用設定（完整說明見 `config.example.toml`）：

| 設定 | 說明 |
|---|---|
| `allowed_user_ids` | bot 模式：允許使用的 user id（空 = 所有人）。user 模式：除了儲存的訊息外，額外處理這些人私訊的檔案。 |
| `proxy` | `socks5://user:pass@host:port` |
| `[transfer] download_connections` / `upload_connections` | 並行連線數（預設 4） |
| `[transfer] pipeline_depth` | 每條連線同時進行中的請求數（預設 8） |
| `[transfer] use_media_dc` | 下載走媒體專用位址（預設開啟） |
| `[transfer] reupload_same_dc` | 已在本帳號 DC 的檔案也重新上傳（預設關閉） |
| `[transfer] split_size_mb` | 分割時每部分的大小上限 |
| `[transfer] max_concurrent_jobs` | 同時處理的檔案數，其餘排隊（預設 1） |
| `[warm]` | 預熱連線池：要保持連線的 DC 與每個 DC 的連線數 |

`config.toml`、`token.txt`、`user.toml`、`*.session` 都已加入 `.gitignore`，請勿提交。session 檔等同於帳號的完整登入憑證。

## 執行

```sh
# bot 模式
tgbot_mvdc5 config.toml

# user 模式：第一次先互動式登入（驗證碼、兩步驟驗證密碼），之後正常啟動
tgbot_mvdc5 login user.toml
tgbot_mvdc5 user.toml
```

user 模式尚未登入時，程式會以結束碼 78 離開（systemd 設定 `RestartPreventExitStatus=78`，不會無限重啟）。

記錄等級用 `RUST_LOG` 控制，建議：

```sh
RUST_LOG=info,grammers=warn,grammers_mtsender::updates_receiver=error
```

## 以 systemd 部署

範例（user 模式；bot 模式把 `user.toml` 換成 `config.toml` 即可，兩者可同時執行）：

```ini
[Unit]
Description=Telegram DC mover (user account)
After=network-online.target
Wants=network-online.target

[Service]
User=tgbot
Group=tgbot
WorkingDirectory=/opt/tgbot_mvdc5
ExecStart=/opt/tgbot_mvdc5/tgbot_mvdc5 /opt/tgbot_mvdc5/user.toml
Restart=always
RestartSec=5
RestartPreventExitStatus=78
Environment=RUST_LOG=info,grammers=warn,grammers_mtsender::updates_receiver=error
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=/opt/tgbot_mvdc5
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

重新部署前請先確認暫存資料夾裡沒有 `*.part` 檔案（代表有任務正在進行），否則重啟會中斷該任務。

## 效能測試（bench）

在不影響正在執行的服務的情況下測試不同的連線配置（使用 session 檔的副本，不會傳送任何訊息）：

```sh
# 下載：<儲存的訊息 id> <連線數> <深度> <分塊 KB> <MiB>
tgbot_mvdc5 bench user.toml 242880 4 8 512 300

# 上傳：把隨機資料上傳到本帳號的 DC，但不附加到任何訊息
tgbot_mvdc5 bench user.toml upload 8 4 400
```

診斷用環境變數：

| 變數 | 作用 |
|---|---|
| `BENCH_DC_OPTIONS=1` | 列出 `help.getConfig` 的所有 DC 位址（含 `media_only`） |
| `BENCH_NO_MEDIA_DC=1` | 不使用媒體位址，用來比較 |
| `BENCH_FLOOD_RECONNECT=1` | FLOOD_WAIT 時改為重連而不是等待 |
| `BENCH_CDN_PROBE=1` | 檢查檔案是否會被導向 CDN DC |

## 授權

未指定。
