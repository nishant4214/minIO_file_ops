from fastapi import FastAPI, File, UploadFile, HTTPException
from minio import Minio  # Correct import from the MinIO library
from minio.error import S3Error
from typing import List
import io
from starlette.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import os

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"], 
)
# minio_client = Minio(
#     endpoint="bhij8w.stackhero-network.com",
#     access_key="s2nxbjXOFV7sSfEUJSyK",
#     secret_key="mvHck28tXEgQlDkxNSBGODX8lHwl9vnLGfe7qE0V",
#     secure=True
# )

minio_client = Minio(
    endpoint=os.getenv("ENDPOINT"),
    access_key=os.getenv("ACCESS_KEY"),
    secret_key=os.getenv("SECRET_KEY"),
    secure=True
)
 
 
@app.post("/create-bucket/{bucket_name}")
async def create_bucket(bucket_name: str):
    try:
        minio_client.make_bucket(bucket_name)
        return {"message": f"Bucket '{bucket_name}' created successfully"}
    except S3Error as err:
        raise HTTPException(status_code=400, detail=str(err))

@app.post("/upload-object/{bucket_name}")
async def upload_object(bucket_name: str, file: UploadFile = File(...)):
    try:
        file_data = file.file.read()
        minio_client.put_object(
            bucket_name,
            file.filename,
            data=io.BytesIO(file_data),
            length=len(file_data)
        )
        return {"message": f"Object '{file.filename}' uploaded successfully to bucket '{bucket_name}'"}
    except S3Error as err:
        raise HTTPException(status_code=400, detail=str(err))

@app.get("/download-object/{bucket_name}/{object_name}")
async def download_object(bucket_name: str, object_name: str):
    try:
        response = minio_client.get_object(bucket_name, object_name)
        return StreamingResponse(response, media_type="application/octet-stream")
    except S3Error as err:
        raise HTTPException(status_code=404, detail=str(err))

@app.get("/list-objects/{bucket_name}")
async def list_objects(bucket_name: str):
    try:
        objects = minio_client.list_objects(bucket_name)
        object_list = [obj.object_name for obj in objects]
        return {"objects": object_list}
    except S3Error as err:
        raise HTTPException(status_code=400, detail=str(err))

@app.delete("/delete-object/{bucket_name}/{object_name}")
async def delete_object(bucket_name: str, object_name: str):
    try:
        minio_client.remove_object(bucket_name, object_name)
        return {"message": f"Object '{object_name}' deleted successfully from bucket '{bucket_name}'"}
    except S3Error as err:
        raise HTTPException(status_code=400, detail=str(err))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=9000)