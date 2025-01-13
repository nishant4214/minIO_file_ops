from fastapi import FastAPI, UploadFile, HTTPException
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload,MediaIoBaseUpload, MediaDownloadProgress
from google.oauth2.service_account import Credentials
from fastapi.responses import StreamingResponse

import os
import io
SERVICE_ACCOUNT_FILE = os.path.join(os.getcwd(), "credentials", "e-sunlight-447709-d9-f872998d3cc5.json")

# Load Google Drive credentials
SCOPES = ["https://www.googleapis.com/auth/drive"]

# Authenticate with Google Drive
credentials = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
drive_service = build("drive", "v3", credentials=credentials)

app = FastAPI()

@app.post("/upload")
async def upload_file(file: UploadFile):
    try:
        file_content = await file.read()
        
        if len(file_content) > 2 * 1024 * 1024:
            raise HTTPException(status_code=400, detail="File size exceeds 2 MB limit")

        allowed_mime_types = {"image/jpeg", "image/png", "application/pdf", "image/jpg"}
        if file.content_type not in allowed_mime_types:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported file type. Allowed types are: {', '.join(allowed_mime_types)}"
            )

        file_metadata = {"name": file.filename}
        media = MediaIoBaseUpload(io.BytesIO(file_content), mimetype=file.content_type)

        response = drive_service.files().create(
            body=file_metadata,
            media_body=media,
            fields="id",
        ).execute()

        return {"file_id": response.get("id"), "message": "File uploaded successfully!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload file: {str(e)}")
    
@app.get("/list-files")
async def list_files():
    try:
        results = drive_service.files().list(
            pageSize=10, fields="files(id, name)"
        ).execute()
        items = results.get("files", [])

        if not items:
            return {"message": "No files found."}

        return {"files": [{"id": item["id"], "name": item["name"]} for item in items]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list files: {str(e)}")
    

@app.get("/download/{file_id}")
async def download_file(file_id: str):
    try:
        # Get file metadata to fetch the name and MIME type
        file_metadata = drive_service.files().get(fileId=file_id, fields="name, mimeType").execute()
        file_name = file_metadata.get("name", "downloaded_file")
        mime_type = file_metadata.get("mimeType", "application/octet-stream")

        # Prepare the download request
        request = drive_service.files().get_media(fileId=file_id)
        file_io = io.BytesIO()
        downloader = MediaIoBaseDownload(file_io, request)

        # Perform the download
        done = False
        while not done:
            status, done = downloader.next_chunk()
            print(f"Download {int(status.progress() * 100)}% complete.")

        file_io.seek(0)

        # Return the file as a streaming response
        return StreamingResponse(
            file_io,
            media_type=mime_type,
            headers={
                "Content-Disposition": f"attachment; filename={file_name}"
            }
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to download file: {str(e)}")
    
@app.delete("/delete/{file_id}")
async def delete_file(file_id: str):
    try:
        drive_service.files().delete(fileId=file_id).execute()
        return {"message": "File deleted successfully!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete file: {str(e)}")
