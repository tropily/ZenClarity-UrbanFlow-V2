#!/bin/bash

# --- CONFIGURATION (The Unified Mirror) ---
# We sync from the local framework folder to S3 to keep dags/ and scripts/ in sync.
LOCAL_FRAMEWORK="./iceberg_backfill_migration_framework/"
S3_DEST="s3://teo-nyc-taxi/iceberg_backfill_migration_framework/"

echo "----------------------------------------------------------"
echo "🚀 TOWER: Mirroring UNIFIED BACKFILL FRAMEWORK to S3..."
echo "----------------------------------------------------------"

# Mirroring both dags/ and scripts/ in one shot
# We exclude the environment and cache to keep the S3 'Stadium' clean.
aws s3 sync "$LOCAL_FRAMEWORK" "$S3_DEST" \
    --delete \
    --exclude ".venv/*" \
    --exclude "__pycache__/*" \
    --exclude "*.pyc" \
    --exclude ".git/*" \
    --exclude "README.md" \
    --exclude "*.log"

# --- VERIFICATION ---
if [ $? -eq 0 ]; then
    echo ""
    echo "✅ SUCCESS: S3 is now a 1:1 mirror of the unified framework."
    echo "----------------------------------------------------------"
    aws s3 ls "$S3_DEST" --recursive
else
    echo ""
    echo "❌ ERROR: Unified sync failed. Check session or permissions."
    echo "----------------------------------------------------------"
    exit 1
fi