#!/bin/bash

# Start script for Marimo Server
# This script sets up the environment and starts the server

set -e

echo "🚀 Starting NYC DCP Data Engineering Marimo Server..."

# Check if we're in the correct directory
if [ ! -f "server.py" ]; then
    echo "❌ Error: server.py not found. Please run from the marimo-server directory."
    exit 1
fi

# Check Python version
python_version=$(python3 --version 2>&1 | awk '{print $2}')
echo "🐍 Python version: $python_version"

# Install dependencies if requirements.txt is newer than last install
if [ requirements.txt -nt .last_install ] || [ ! -f .last_install ]; then
    echo "📦 Installing/updating dependencies..."
    pip install -r requirements.txt
    touch .last_install
else
    echo "✅ Dependencies up to date"
fi

# Set default environment variables
export MARIMO_HOST=${MARIMO_HOST:-"0.0.0.0"}
export MARIMO_PORT=${MARIMO_PORT:-8080}

echo "🌐 Server will start on: http://${MARIMO_HOST}:${MARIMO_PORT}"
echo "📂 Repository root: $(realpath ../../..)"
echo "📝 Notebooks directory: $(realpath ./notebooks)"

# Check if notebooks directory exists and has content
if [ ! -d "./notebooks" ]; then
    echo "❌ Error: notebooks directory not found"
    exit 1
fi

notebook_count=$(find ./notebooks -name "*.py" | wc -l)
echo "📊 Found $notebook_count notebook(s)"

if [ $notebook_count -eq 0 ]; then
    echo "⚠️  Warning: No notebooks found in ./notebooks/"
fi

echo ""
echo "📖 Available notebooks:"
for notebook in ./notebooks/*.py; do
    if [ -f "$notebook" ]; then
        basename=$(basename "$notebook" .py)
        echo "  - /$basename -> $notebook"
    fi
done

echo ""
echo "🎯 Starting server..."
echo "   Press Ctrl+C to stop"
echo "   Visit http://localhost:${MARIMO_PORT} to access notebooks"
echo ""

# Start the server
python server.py