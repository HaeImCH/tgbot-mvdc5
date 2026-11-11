# tgbot_mvdc5

英文版請見 [README.en.md](README.en.md)。

## 1. 用途

tgbot_mvdc5 把 Telegram 檔案搬到你帳號所在的資料中心（DC）。

Telegram 把每個檔案存放在一個 DC。檔案在其他 DC 時，從你所在的地區下載可能很慢。tgbot_mvdc5 從檔案所在的 DC 下載檔案。然後把檔案重新上傳到你帳號所在的 DC。然後把新檔案傳給你。

處理期間，tgbot_mvdc5 用一則狀態訊息顯示進度。狀態訊息使用繁體中文。

## 2. 說明

### 2.1 帳號模式

tgbot_mvdc5 有兩種帳號模式：

| 模式 | 登入方式 | 接受的檔案 | 上傳上限 |
|---|---|---|---|
| `bot` | bot token | 使用者在私訊中傳給 bot 的檔案 | 2000 MiB |
| `user` | 你的 Telegram 帳號 | 「儲存的訊息」中的檔案，以及 `allowed_user_ids` 中的使用者私訊給你的檔案 | 4000 MiB（Premium） |

在 `user` 模式下，tgbot_mvdc5 不回覆文字訊息。

建議使用 `user` 模式，並使用 Telegram Premium 帳號。這樣速度最快：

- Telegram 會限制 bot 帳號的下載速度。使用者帳號沒有這個限制。
- Premium 帳號可以上傳最大 4000 MiB 的檔案。因此大部分檔案不需要分割。

### 2.2 功能

- 每次傳輸使用 4 條連線。你可以變更這個數字。
- 每條連線同時送出 8 個請求。你可以變更這個數字。
- 每條下載連線使用自己的授權。這樣可以減少 FLOOD_WAIT 錯誤。
- 下載時，tgbot_mvdc5 使用該 DC 的媒體專用位址。Telegram Desktop 也使用這個位址。
- 媒體專用位址連不上時，tgbot_mvdc5 改用一般位址。
- tgbot_mvdc5 可以透過 SOCKS5 代理連線。所有連線都經過代理。
- 檔案大於上傳上限時，tgbot_mvdc5 把檔案分割成多個部分。各部分的檔名以 `.001`、`.002` 等結尾。
- tgbot_mvdc5 對每個 DC 保持閒置連線。因此新的傳輸可以立即開始。
- tgbot_mvdc5 在記錄中記下每個請求。記錄顯示傳送者、檔名與結果。

### 2.3 實測速度

測試條件：1.93 GB 的檔案位於 DC4，帳號位於 DC5，伺服器位於台北，使用 SOCKS5 代理。每次傳輸使用 4 條連線，每條連線同時 8 個請求。

#### bot 帳號與 Premium 使用者帳號比較

| 操作 | `bot` 模式 | `user` 模式（Premium） |
|---|---|---|
| 從 DC4 下載 | 約 14.6 MB/s，1385 次 FLOOD_WAIT | 約 64 MB/s，0 次 FLOOD_WAIT |
| 上傳到 DC5 | 約 12 MB/s，每 400 MB 有 20–44 次 FLOOD_WAIT | 約 24–27 MB/s，0 次 FLOOD_WAIT |
| 1.93 GB 檔案總耗時 | 約 5 分鐘（推算） | 約 1 分 50 秒（實測） |
| 單一檔案上傳上限 | 2000 MiB | 4000 MiB |

使用 Premium 帳號的 `user` 模式，下載約快 4 倍，上傳約快 2 倍。

#### Premium 使用者帳號的其他測量

| 操作 | 速度 |
|---|---|
| 從 DC4 下載，一般位址（`use_media_dc = false`） | 約 13 MB/s |
| 從 DC5 下載，媒體專用位址（`reupload_same_dc = true`） | 約 104 MB/s |

### 2.4 限制

- 上傳速度上限約 27 MB/s，這是 Telegram 端的限制。增加連線數不會提高速度。
- Telegram 會限制 bot 帳號的下載速度。大檔案請使用 `user` 模式與 Premium 帳號。
- DC3 沒有媒體專用位址。從 DC3 下載比較慢。

## 3. 建置程序

1. 安裝 Rust 工具鏈。
2. 編譯程式：

   ```sh
   cargo build --release
   ```

3. 執行測試：

   ```sh
   cargo test
   ```

4. 要編譯 Linux amd64 版本，先安裝 `cross` 與 Docker。然後執行這個指令：

   ```sh
   CROSS_CONTAINER_OPTS="--platform linux/amd64" \
     cross build --release --target x86_64-unknown-linux-gnu
   ```

備註：`vendor/grammers-mtsender` 是修改過的 grammers-mtsender 0.12.0。修改內容是把 `Sender::enqueue_body` 設為公開。請參閱 `vendor/grammers-mtsender/PATCH.md`。

## 4. 設定程序

1. 複製範例檔：

   ```sh
   cp config.example.toml config.toml
   ```

2. 到 <https://my.telegram.org> 取得 `api_id` 與 `api_hash`。
3. 把 `api_id` 與 `api_hash` 寫入 `config.toml`。
4. `bot` 模式：把 bot token 放入 `token.txt`。
5. `user` 模式：設定 `account = "user"`。
6. `user` 模式：設定一個與 bot 不同的 `session_file`。
7. 使用代理時，設定 `proxy = "socks5://user:password@host:port"`。

警告：不要把 `config.toml`、`token.txt`、`user.toml` 或 `*.session` 放入 git。session 檔可以完整存取帳號。

### 4.1 常用設定

| 設定 | 作用 |
|---|---|
| `allowed_user_ids` | `bot` 模式：可以使用 bot 的使用者。空清單表示所有使用者都可以使用。`user` 模式：tgbot_mvdc5 也會搬移這些使用者私訊的檔案。 |
| `[transfer] download_connections` | 下載連線數。預設為 4。 |
| `[transfer] upload_connections` | 上傳連線數。預設為 4。 |
| `[transfer] pipeline_depth` | 每條連線同時進行的請求數。預設為 8。 |
| `[transfer] use_media_dc` | 下載時使用媒體專用位址。預設為 `true`。 |
| `[transfer] reupload_same_dc` | 已在帳號所在 DC 的檔案也重新搬移。預設為 `false`。 |
| `[transfer] split_size_mb` | 每個部分的最大大小，單位 MiB。 |
| `[transfer] max_concurrent_jobs` | 同時搬移的檔案數。預設為 1。 |
| `[warm]` | 要保持閒置連線的 DC，以及每個 DC 的連線數。 |

所有設定請參閱 `config.example.toml`。

## 5. 操作程序

### 5.1 以 bot 模式啟動

1. 啟動程式：

   ```sh
   tgbot_mvdc5 config.toml
   ```

### 5.2 以 user 模式啟動

1. 登入一次：

   ```sh
   tgbot_mvdc5 login user.toml
   ```

2. 輸入 Telegram 傳來的驗證碼。
3. 帳號有兩步驟驗證時，輸入密碼。
4. 啟動程式：

   ```sh
   tgbot_mvdc5 user.toml
   ```

備註：帳號尚未登入時，程式以結束碼 78 停止。

### 5.3 搬移檔案

1. 傳送或轉傳檔案：
   - `bot` 模式：傳給 bot。
   - `user` 模式：轉傳到「儲存的訊息」。
2. 查看狀態訊息。狀態訊息顯示階段、進度與速度。
3. 傳輸完成時，tgbot_mvdc5 以回覆的方式傳送新檔案。

### 5.4 設定記錄等級

1. 設定 `RUST_LOG` 環境變數。建議使用這個值：

   ```sh
   RUST_LOG=info,grammers=warn,grammers_mtsender::updates_receiver=error
   ```

## 6. 安裝為 systemd 服務

1. 把程式與設定檔複製到 `/opt/tgbot_mvdc5`。
2. 建立單元檔，例如 `/etc/systemd/system/tgbot_mvdc5-user.service`：

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

3. `bot` 模式：建立第二個單元檔。用 `config.toml` 取代 `user.toml`。
4. 啟動服務：

   ```sh
   systemctl daemon-reload
   systemctl enable --now tgbot_mvdc5-user
   ```

注意：重新啟動服務前，確認暫存資料夾中沒有 `*.part` 檔案。`*.part` 檔案表示傳輸正在進行。重新啟動會中斷傳輸。

## 7. 速度測試程序（bench）

bench 指令不會影響正在執行的服務。它使用 session 檔的副本。它不傳送任何訊息。

1. 測試下載時，使用這個指令：

   ```sh
   tgbot_mvdc5 bench user.toml <訊息_id> <連線數> <深度> <分塊_KB> <MiB>
   ```

   檔案必須在「儲存的訊息」中。例如：

   ```sh
   tgbot_mvdc5 bench user.toml 242880 4 8 512 300
   ```

2. 測試上傳時，使用這個指令：

   ```sh
   tgbot_mvdc5 bench user.toml upload <連線數> <深度> <MiB>
   ```

   上傳使用隨機資料。沒有訊息使用這些資料，所以 Telegram 會刪除它們。

### 7.1 診斷用變數

| 變數 | 作用 |
|---|---|
| `BENCH_DC_OPTIONS=1` | 顯示 `help.getConfig` 中所有的 DC 位址。 |
| `BENCH_NO_MEDIA_DC=1` | 停用媒體專用位址，用於比較。 |
| `BENCH_FLOOD_RECONNECT=1` | 遇到 FLOOD_WAIT 時重新連線，不等待。 |
| `BENCH_CDN_PROBE=1` | 顯示 Telegram 是否把檔案導向 CDN DC。 |

## 8. 故障排除

| 問題 | 可能原因 | 處理方式 |
|---|---|---|
| 程式以結束碼 78 停止。 | `user` 帳號尚未登入。 | 執行 5.2 節的步驟。 |
| 下載很慢，記錄中有很多 FLOOD_WAIT 錯誤。 | 帳號是 bot，或該 DC 沒有媒體專用位址。 | 使用 `user` 模式。確認 `use_media_dc = true`。 |
| 記錄顯示 `falling back to the regular address`。 | 媒體專用位址連不上。 | 不需要處理。tgbot_mvdc5 會使用一般位址。 |
| 狀態訊息顯示傳輸已中斷。 | 傳輸期間服務重新啟動。 | 重新傳送檔案。 |

## 9. 授權

本軟體採用 MIT 授權。請參閱 [LICENSE](LICENSE)。
