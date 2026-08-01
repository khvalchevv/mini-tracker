# mini-tracker

Telegram bot that watches the spread between any two prices — DexScreener pools
and/or CEX spot pairs — and pings you when it crosses your threshold.

## Local run

```bash
python -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env                 # fill TG_BOT_TOKEN
# put your rotating proxies (one per line) into proxies.txt (optional)
python main.py
```

## Server deploy (Ubuntu / Debian, systemd)

```bash
# on the server, as the user that will run the bot
sudo apt update && sudo apt install -y python3-venv git

git clone https://github.com/<you>/mini-tracker.git
cd mini-tracker

python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

cp .env.example .env
nano .env                            # set TG_BOT_TOKEN, TG_ALLOWED_USERS (optional)
nano proxies.txt                     # paste rotating proxies, one per line (optional)

# --- systemd unit ---
sudo tee /etc/systemd/system/mini-tracker.service >/dev/null <<'EOF'
[Unit]
Description=Mini spread tracker TG bot
After=network-online.target

[Service]
Type=simple
WorkingDirectory=/home/USER/mini-tracker
ExecStart=/home/USER/mini-tracker/.venv/bin/python main.py
Restart=on-failure
RestartSec=5
StandardOutput=append:/home/USER/mini-tracker/bot.log
StandardError=append:/home/USER/mini-tracker/bot.err

[Install]
WantedBy=multi-user.target
EOF

# replace USER above with your actual username, then:
sudo systemctl daemon-reload
sudo systemctl enable --now mini-tracker
sudo systemctl status mini-tracker
tail -f /home/USER/mini-tracker/bot.log
```

## Update

```bash
cd ~/mini-tracker
git pull
sudo systemctl restart mini-tracker
```
