#!/bin/bash
# Setup script for Carlisle Energy app environment variables
# This script helps you create a .env file from the template

set -e

echo "=========================================="
echo "Carlisle Energy - Environment Setup"
echo "=========================================="
echo ""

# Check if .env already exists
if [ -f .env ]; then
    echo "⚠️  .env file already exists!"
    read -p "Do you want to overwrite it? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Keeping existing .env file. Exiting."
        exit 0
    fi
    echo "Backing up existing .env to .env.backup..."
    cp .env .env.backup
fi

# Copy template
if [ ! -f env.example ]; then
    echo "❌ Error: env.example not found!"
    exit 1
fi

cp env.example .env
echo "✅ Created .env file from template"
echo ""
echo "📝 Next steps:"
echo "   1. Open .env in your editor"
echo "   2. Fill in your actual API keys and credentials"
echo "   3. Save the file"
echo ""
echo "Required for bill intake features:"
echo "   - DATABASE_URL (PostgreSQL connection string)"
echo "   - XAI_API_KEY (from https://console.x.ai/)"
echo ""
echo "Optional but recommended:"
echo "   - GOOGLE_PLACES_API_KEY (for address autocomplete)"
echo "   - DROPBOX_* credentials (for CSV exports)"
echo ""
echo "💡 Tip: You can test the app without all env vars -"
echo "   SiteWalk features work without any configuration!"

