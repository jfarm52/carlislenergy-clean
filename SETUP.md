# Environment Setup Guide

This guide helps you configure environment variables for the Carlisle Energy app.

## Quick Start

### Option 1: Use the setup script (recommended)

```bash
./setup_env.sh
```

Then edit `.env` and fill in your credentials.

### Option 2: Manual setup

```bash
# Copy the template
cp .env.example .env

# Edit .env with your favorite editor
nano .env
# or
code .env
```

## Environment Variables Explained

### Core Features

- **`UTILITY_BILLS_ENABLED`**: Set to `"true"` or `"false"` (default: `true`)
  - Controls whether bill upload/extraction features are available
  - If `false`, bill-related endpoints return 403 errors

### Bill Intake (Required for bill features)

- **`DATABASE_URL`**: PostgreSQL connection string
  - Format: `postgresql://user:password@host:port/database`
  - Example: `postgresql://postgres:mypassword@localhost:5432/carlisle_bills`
  - **Required** if you want to use bill upload/extraction

- **`XAI_API_KEY`**: xAI API key for Grok models
  - Get your key from: https://console.x.ai/
  - **Required** for bill PDF/image extraction

### Optional Integrations

- **`GOOGLE_PLACES_API_KEY`**: Google Places API key
  - Get from: https://console.cloud.google.com/apis/credentials
  - Enables address autocomplete and business search
  - **Optional** - app works without it, but autocomplete won't work

- **`DROPBOX_APP_KEY`**, **`DROPBOX_APP_SECRET`**, **`DROPBOX_REFRESH_TOKEN`**
  - Get from: https://www.dropbox.com/developers/apps
  - Used for CSV exports and proposal folder creation
  - **Optional** - app works without it, but Dropbox export won't work

## Testing Without Full Setup

The app runs **without any environment variables** configured! Here's what works:

✅ **SiteWalk features** (core app):
- Create/edit projects
- Add equipment data
- Save to local JSON files
- All basic functionality

❌ **Bill intake** (requires `DATABASE_URL` + `XAI_API_KEY`):
- Bill upload endpoints return errors
- Extraction won't work

❌ **Google Places** (requires `GOOGLE_PLACES_API_KEY`):
- Address autocomplete won't work
- Business search disabled

❌ **Dropbox export** (requires Dropbox credentials):
- CSV export to Dropbox won't work

## Installing Dependencies

After setting up `.env`, install Python dependencies:

```bash
# Create virtualenv (recommended)
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Running the App

```bash
python main.py
```

Then open `http://localhost:5000` in your browser.

## Verifying Configuration

The app will print messages on startup:

- `[ENV] Loaded environment variables from .env file` - ✅ .env loaded successfully
- `[ENV] python-dotenv not installed` - ⚠️ Install with `pip install python-dotenv`
- `[ENV] Warning: Could not load .env file` - ⚠️ Check .env file syntax

## Security Notes

- ✅ `.env` is already in `.gitignore` - your secrets won't be committed
- ✅ Never commit `.env` to git
- ✅ Use `.env.example` as a template (it has no real secrets)
- ✅ Rotate API keys if they're ever exposed

## Getting API Keys

### xAI API Key
1. Go to https://console.x.ai/
2. Sign up or log in
3. Navigate to API Keys section
4. Create a new key
5. Copy and paste into `.env` as `XAI_API_KEY=your_key_here`

### Google Places API Key
1. Go to https://console.cloud.google.com/
2. Create a new project (or select existing)
3. Enable "Places API" and "Geocoding API"
4. Go to Credentials → Create Credentials → API Key
5. Copy and paste into `.env` as `GOOGLE_PLACES_API_KEY=your_key_here`

### Dropbox Credentials
1. Go to https://www.dropbox.com/developers/apps
2. Create a new app
3. Use "Scoped access" → "Full Dropbox"
4. Copy App Key, App Secret
5. Generate refresh token (or use access token)
6. Add all three to `.env`

### PostgreSQL Database
If you don't have PostgreSQL installed locally:

**Option 1: Local PostgreSQL**
```bash
# macOS (Homebrew)
brew install postgresql
brew services start postgresql
createdb carlisle_bills

# Then set DATABASE_URL in .env:
# DATABASE_URL=postgresql://$(whoami)@localhost:5432/carlisle_bills
```

**Option 2: Docker (recommended for dev)**
```bash
docker compose up -d
```
Then set `DATABASE_URL` in `local.env` (recommended) or `.env`:
`postgresql://carlisle:carlisle@localhost:5432/carlisle_bills`

**Option 2: Cloud Database**
- Use services like Supabase, Neon, or Railway
- They provide connection strings in the format:
  `postgresql://user:password@host:port/database`
- Copy the connection string to `.env` as `DATABASE_URL`

## Troubleshooting

### "DATABASE_URL not configured" error
- Make sure `.env` file exists and has `DATABASE_URL=` set
- Check that the connection string format is correct
- Verify PostgreSQL is running (if local)

### "XAI_API_KEY environment variable not set" error
- Check `.env` file has `XAI_API_KEY=your_key`
- Verify the key is valid at https://console.x.ai/
- Make sure there are no extra spaces or quotes around the key

### Google Places autocomplete not working
- Check browser console for API errors
- Verify `GOOGLE_PLACES_API_KEY` is set in `.env`
- Check that Places API is enabled in Google Cloud Console
- Verify API key restrictions allow your domain/localhost

### .env file not loading
- Make sure `.env` is in the project root (same directory as `app.py`)
- Check file permissions: `ls -la .env`
- Verify `python-dotenv` is installed: `pip list | grep dotenv`
- Check app startup logs for `[ENV]` messages

