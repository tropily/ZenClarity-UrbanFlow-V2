#!/bin/bash

# --- CONFIGURATION ---
# The local folder we trust as the "Master"
LOCAL_INFRA="./infrastructure/"
# The S3 destination where Glue and EMR look for code
S3_DEST="s3://teo-nyc-taxi/scripts/"

echo "----------------------------------------------------------"
echo "🚀 TOWER: Starting Deployment Sync to S3..."
echo "----------------------------------------------------------"

# --- THE SYNC OPERATION ---
# --delete: Removes files in S3 that do not exist in your local infrastructure/ folder.
# --exclude: Ensures we don't accidentally delete your legacy archive if you kept it in the same path.
aws s3 sync $LOCAL_INFRA $S3_DEST \
    --delete \
    --exclude "archive_legacy/*" \
    --exclude ".venv/*" \
    --exclude "__pycache__/*"

# --- VERIFICATION ---
if [ $? -eq 0 ]; then
    echo ""
    echo "✅ SUCCESS: S3 is now a mirror of your local Infrastructure."
    echo "----------------------------------------------------------"
    echo "📂 CURRENT S3 STRUCTURE:"
    aws s3 ls $S3_DEST --recursive
    echo "----------------------------------------------------------"
    echo "Time: $(date)"
else
    echo ""
    echo "❌ ERROR: Sync failed. Check your AWS Session or S3 permissions."
    echo "----------------------------------------------------------"
    exit 1
fi