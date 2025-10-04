import os
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

# ---------- Config ----------
CLIENT_SECRET_PATH = ""
BATCH_SIZE = 100  # Number of files to fetch per API call
# ----------------------------

print("⚠️  WARNING: This script will DELETE ALL FILES from your Google Drive!")
print("=" * 70)
confirmation = input("Type 'DELETE ALL' to confirm (case-sensitive): ")

if confirmation != "DELETE ALL":
    print("❌ Deletion cancelled. Exiting...")
    exit(0)

print("\n" + "=" * 70)
print("🔐 Authenticating with Google Drive...")
gauth = GoogleAuth()
gauth.LoadClientConfigFile(CLIENT_SECRET_PATH)
try:
    gauth.LocalWebserverAuth()
except Exception:
    gauth.CommandLineAuth()
drive = GoogleDrive(gauth)
print("✅ Google Drive authenticated\n")

print("=" * 70)
print("🔍 Fetching all files from Google Drive...")
print("=" * 70)

# List all files (not trashed)
all_files = []
page_token = None

while True:
    try:
        query_params = {
            'q': "trashed=false",  # Only non-trashed files
            'maxResults': BATCH_SIZE,
            'pageToken': page_token
        }
        file_list = drive.ListFile(query_params).GetList()

        if not file_list:
            break

        all_files.extend(file_list)
        print(f"   Fetched {len(all_files)} files so far...")

        # Check if there are more pages
        if len(file_list) < BATCH_SIZE:
            break

        # Get next page token (if pagination is needed)
        page_token = None  # PyDrive handles pagination automatically

    except Exception as e:
        print(f"⚠️  Error fetching files: {e}")
        break

total_files = len(all_files)
print(f"\n✅ Found {total_files} files to delete\n")

if total_files == 0:
    print("🎉 No files to delete! Your Drive is already empty.")
    exit(0)

print("=" * 70)
print(f"🗑️  Deleting {total_files} files...")
print("=" * 70)

deleted_count = 0
failed_count = 0

for i, file in enumerate(all_files, start=1):
    try:
        file_title = file.get('title', 'Unknown')
        file_id = file.get('id')

        print(f"[{i}/{total_files}] Deleting: {file_title} (ID: {file_id})")

        # Delete the file
        file.Delete()
        deleted_count += 1

    except Exception as e:
        print(f"[{i}/{total_files}] ❌ Failed to delete {file_title}: {e}")
        failed_count += 1

print("\n" + "=" * 70)
print("🎉 Deletion complete!")
print("=" * 70)
print(f"✅ Successfully deleted: {deleted_count} files")
print(f"❌ Failed to delete: {failed_count} files")
print(f"📊 Total processed: {total_files} files")
print("=" * 70)

if failed_count > 0:
    print("\n⚠️  Note: Some files failed to delete. They might be:")
    print("   - Shared files (not owned by you)")
    print("   - Files in shared drives")
    print("   - Files with restricted permissions")
    print("   You may need to manually delete these from the Google Drive web interface.")
