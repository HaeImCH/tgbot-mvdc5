# tgbot_mvdc5

A Traditional Chinese version is in [README.md](README.md).

## 1. Purpose

tgbot_mvdc5 moves Telegram files to the data center (DC) of your account.

Telegram keeps each file on one DC. When the file is on a different DC, the download from your area can be slow. tgbot_mvdc5 downloads the file from its DC. Then it uploads the file again to the DC of your account. Then it sends the new file to you.

During the work, tgbot_mvdc5 shows the progress in one status message. The status message is in Traditional Chinese.

## 2. Description

### 2.1 Account modes

tgbot_mvdc5 has two account modes:

| Mode | Login | Files that it accepts | Upload limit |
|---|---|---|---|
| `bot` | Bot token | Files that users send to the bot in a private chat | 2000 MiB |
| `user` | Your Telegram account | Files in your Saved Messages, and files from the users in `allowed_user_ids` | 4000 MiB (Premium) |

In `user` mode, tgbot_mvdc5 does not reply to text messages.

We recommend `user` mode with a Telegram Premium account. This mode gives the highest speeds:

- Telegram limits the download speed of bot accounts. A user account does not have this limit.
- A Premium account can upload files of up to 4000 MiB. Thus tgbot_mvdc5 does not divide most files.

### 2.2 Functions

- tgbot_mvdc5 uses 4 connections for each transfer. You can change this number.
- Each connection sends 8 requests at the same time. You can change this number.
- Each download connection has its own authorization. This decreases the FLOOD_WAIT errors.
- For downloads, tgbot_mvdc5 uses the media-only address of the DC. Telegram Desktop uses the same address.
- If the media-only address does not connect, tgbot_mvdc5 uses the usual address.
- tgbot_mvdc5 can connect through a SOCKS5 proxy. All connections use the proxy.
- If a file is larger than the upload limit, tgbot_mvdc5 divides it into parts. The part names end in `.001`, `.002`, and so on.
- tgbot_mvdc5 keeps idle connections to each DC. Thus a new transfer can start immediately.
- tgbot_mvdc5 records each request in the log. The log shows the sender, the file name, and the result.

### 2.3 Measured speeds

Test conditions: a 1.93 GB file on DC4, accounts on DC5, a server in Taipei, and a SOCKS5 proxy. Each transfer uses 4 connections with 8 requests on each connection.

#### Bot account compared with a Premium user account

| Operation | `bot` mode | `user` mode, Premium |
|---|---|---|
| Download from DC4 | Approximately 14.6 MB/s, 1385 FLOOD_WAIT errors | Approximately 64 MB/s, 0 FLOOD_WAIT errors |
| Upload to DC5 | Approximately 12 MB/s, 20 to 44 FLOOD_WAIT errors for each 400 MB | Approximately 24 to 27 MB/s, 0 FLOOD_WAIT errors |
| Total time for the 1.93 GB file | Approximately 5 minutes (calculated) | Approximately 1 minute 50 seconds (measured) |
| Upload limit for each file | 2000 MiB | 4000 MiB |

The `user` mode with a Premium account is approximately 4 times faster for downloads and 2 times faster for uploads.

#### Other measurements with the Premium user account

| Operation | Speed |
|---|---|
| Download from DC4, usual address (`use_media_dc = false`) | Approximately 13 MB/s |
| Download from DC5, media-only address (`reupload_same_dc = true`) | Approximately 104 MB/s |

### 2.4 Limits

- The upload speed limit of approximately 27 MB/s is on the Telegram side. More connections do not increase the speed.
- Telegram limits the download speed of bot accounts. For large files, use `user` mode with a Premium account.
- DC3 has no media-only address. Downloads from DC3 are slower.

## 3. Build procedure

1. Install the Rust toolchain.
2. Compile the program:

   ```sh
   cargo build --release
   ```

3. Do the tests:

   ```sh
   cargo test
   ```

4. To compile for Linux amd64, install `cross` and Docker. Then use this command:

   ```sh
   CROSS_CONTAINER_OPTS="--platform linux/amd64" \
     cross build --release --target x86_64-unknown-linux-gnu
   ```

NOTE: The directory `vendor/grammers-mtsender` contains a changed copy of grammers-mtsender 0.12.0. The change makes `Sender::enqueue_body` public. Refer to `vendor/grammers-mtsender/PATCH.md`.

## 4. Configuration procedure

1. Copy the example file:

   ```sh
   cp config.example.toml config.toml
   ```

2. Get an `api_id` and an `api_hash` from <https://my.telegram.org>.
3. Write the `api_id` and the `api_hash` in `config.toml`.
4. For `bot` mode, put the bot token in `token.txt`.
5. For `user` mode, set `account = "user"`.
6. For `user` mode, set a `session_file` that is different from the bot session file.
7. If you use a proxy, set `proxy = "socks5://user:password@host:port"`.

WARNING: DO NOT PUT `config.toml`, `token.txt`, `user.toml`, OR `*.session` IN GIT. A session file gives full access to the account.

### 4.1 Frequently used settings

| Setting | Function |
|---|---|
| `allowed_user_ids` | `bot` mode: the users that can use the bot. An empty list lets all users use it. `user` mode: the users whose private files tgbot_mvdc5 also moves. |
| `[transfer] download_connections` | The number of download connections. The default is 4. |
| `[transfer] upload_connections` | The number of upload connections. The default is 4. |
| `[transfer] pipeline_depth` | The number of requests on each connection at the same time. The default is 8. |
| `[transfer] use_media_dc` | Use the media-only address for downloads. The default is `true`. |
| `[transfer] reupload_same_dc` | Also move files that are already on the DC of the account. The default is `false`. |
| `[transfer] split_size_mb` | The maximum size of each part, in MiB. |
| `[transfer] max_concurrent_jobs` | The number of files that tgbot_mvdc5 moves at the same time. The default is 1. |
| `[warm]` | The DCs and the number of idle connections for each DC. |

For all settings, refer to `config.example.toml`.

## 5. Operation procedure

### 5.1 Start in bot mode

1. Start the program:

   ```sh
   tgbot_mvdc5 config.toml
   ```

### 5.2 Start in user mode

1. Do the login one time:

   ```sh
   tgbot_mvdc5 login user.toml
   ```

2. Type the login code from Telegram.
3. If the account has two-step verification, type the password.
4. Start the program:

   ```sh
   tgbot_mvdc5 user.toml
   ```

NOTE: If the account has no login, the program stops with exit code 78.

### 5.3 Move a file

1. Send or forward the file:
   - In `bot` mode, send it to the bot.
   - In `user` mode, forward it to your Saved Messages.
2. Read the status message. It shows the stage, the progress, and the speed.
3. When the transfer is complete, tgbot_mvdc5 sends the new file as a reply.

### 5.4 Set the log level

1. Set the `RUST_LOG` environment variable. This value is recommended:

   ```sh
   RUST_LOG=info,grammers=warn,grammers_mtsender::updates_receiver=error
   ```

## 6. Installation as a systemd service

1. Copy the program and the configuration file to `/opt/tgbot_mvdc5`.
2. Make a unit file, for example `/etc/systemd/system/tgbot_mvdc5-user.service`:

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

3. For `bot` mode, make a second unit file. Use `config.toml` in place of `user.toml`.
4. Start the service:

   ```sh
   systemctl daemon-reload
   systemctl enable --now tgbot_mvdc5-user
   ```

CAUTION: Before you restart the service, make sure that the temporary directory has no `*.part` files. A `*.part` file shows that a transfer is in progress. A restart stops the transfer.

## 7. Speed test procedure (bench)

The bench command does not change the services that run. It uses a copy of the session file. It does not send messages.

1. To test a download, use this command:

   ```sh
   tgbot_mvdc5 bench user.toml <message_id> <connections> <depth> <chunk_kb> <mib>
   ```

   The file must be in your Saved Messages. For example:

   ```sh
   tgbot_mvdc5 bench user.toml 242880 4 8 512 300
   ```

2. To test an upload, use this command:

   ```sh
   tgbot_mvdc5 bench user.toml upload <connections> <depth> <mib>
   ```

   The upload uses random data. Telegram deletes the uploaded data because no message uses it.

### 7.1 Diagnostic variables

| Variable | Function |
|---|---|
| `BENCH_DC_OPTIONS=1` | Shows all DC addresses from `help.getConfig`. |
| `BENCH_NO_MEDIA_DC=1` | Disables the media-only address, for comparison. |
| `BENCH_FLOOD_RECONNECT=1` | On FLOOD_WAIT, reconnects instead of waiting. |
| `BENCH_CDN_PROBE=1` | Shows if Telegram sends the file to a CDN DC. |

## 8. Troubleshooting

| Problem | Possible cause | Action |
|---|---|---|
| The program stops with exit code 78. | The `user` account has no login. | Do step 5.2. |
| Downloads are slow and the log shows many FLOOD_WAIT errors. | The account is a bot, or the DC has no media-only address. | Use `user` mode. Make sure that `use_media_dc = true`. |
| The log shows `falling back to the regular address`. | The media-only address did not connect. | No action is necessary. tgbot_mvdc5 uses the usual address. |
| The status message shows that the transfer stopped. | The service restarted during the transfer. | Send the file again. |

## 9. License

This software has the MIT License. Refer to [LICENSE](LICENSE).
