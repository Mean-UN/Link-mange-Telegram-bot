# Telegram Link Bot

A Telegram bot to store titles, episodes, and links. Admins manage content via buttons, users browse and open links.

## Features

- Add titles, episodes, and links
- Bulk add episodes by pasting many pairs (supports multi-message input)
- Copy all episodes (name + link)
- Role system: main admins from `.env` and added admins (limited to their own data)
- Pagination for large lists
- Auto-delete bot messages after 2 minutes

## Setup

1) Create a bot with @BotFather and get the token.
2) Create a virtual environment and install dependencies:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

3) Create a `.env` file based on `.env.example` and set:

- `BOT_TOKEN`
- `ADMIN_IDS` (comma-separated Telegram user IDs)
- `DB_PATH` (optional)

4) Run the bot:

```powershell
python bot.py
```

## Commands

User:
- `/start` - welcome & how to use
- `/help` - show commands
- `/linkmanga` - browse titles
- `/listep 1-10` - generate episode labels
- `/getuserid` - get user ID (reply to a user to get their ID)
- `/donateadmin` - show donation QR

Admin:
- `/admin` - admin panel
- `/addadmin <user_id>` - add admin (main admins only)
- `/removeadmin <user_id>` - remove admin (main admins only)
- `/listadmin` - list admins (main admins only)
- `/done` - finish bulk add input
- `/cancel` - cancel current admin input

## Admin Rules

- Main admins are defined in `.env` and can manage all data.
- Added admins can only manage titles/episodes they created.
- Added admins cannot add/remove other admins.

## Notes

- If you want the donation QR command to work, place `donate_qr.jpg` in the project folder.
- SQLite DB is stored in `linkbot.db` by default.
