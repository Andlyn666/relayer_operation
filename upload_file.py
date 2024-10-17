import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

def upload_to_gdrive(file_name, folder_id=None):
    """Uploads a file to Google Drive using a service account and makes it public."""
    # Authenticate using the service account
    SCOPES = ['https://www.googleapis.com/auth/drive']
    SERVICE_ACCOUNT_FILE = 'service_account.json'  # Replace with the path to your service account key file

    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build('drive', 'v3', credentials=credentials)

    # Search for the file by name in the specified folder
    query = f"name='{os.path.basename(file_name)}'"
    if folder_id:
        query += f" and '{folder_id}' in parents"
    results = service.files().list(q=query, spaces='drive').execute()
    items = results.get('files', [])

    if items:
        # Update the existing file
        file_id = items[0]['id']
        media = MediaFileUpload(file_name, resumable=True)
        updated_file = service.files().update(fileId=file_id, media_body=media).execute()
        print(f"File URL: https://drive.google.com/file/d/{updated_file['id']}/view")
    else:
        # Upload the new file
        file_metadata = {'name': os.path.basename(file_name)}
        if folder_id:
            file_metadata['parents'] = [folder_id]
        media = MediaFileUpload(file_name, resumable=True)
        new_file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        file_id = new_file['id']
        print(f"File URL: https://drive.google.com/file/d/{file_id}/view")

    # Make the file public
    permission = {
        'type': 'anyone',
        'role': 'writer',
    }
    service.permissions().create(fileId=file_id, body=permission).execute()

    