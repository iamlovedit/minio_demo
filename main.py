from fastapi import FastAPI, UploadFile, File, HTTPException
from minio import Minio
from minio.error import S3Error
import os

# 初始化 FastAPI 应用
app = FastAPI()

# MinIO 客户端配置
minio_client = Minio(
    "minio:9000",  # MinIO 服务器地址
    access_key="admin",  # 访问密钥
    secret_key="12345678",  # 秘密密钥
    secure=False  # 如果使用 HTTPS，设置为 True
)

# MinIO 存储桶名称
BUCKET_NAME = "dev"

# 确保存储桶存在
def ensure_bucket_exists():
    if not minio_client.bucket_exists(BUCKET_NAME):
        minio_client.make_bucket(BUCKET_NAME)

# 上传文件到 MinIO
@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    ensure_bucket_exists()
    try:
        # 保存文件到本地临时文件
        file_path = f"temp_{file.filename}"
        with open(file_path, "wb") as buffer:
            buffer.write(await file.read())

        # 上传文件到 MinIO
        minio_client.fput_object(BUCKET_NAME, file.filename, file_path)
        os.remove(file_path)  # 删除临时文件

        return {"message": f"File '{file.filename}' uploaded successfully."}
    except S3Error as e:
        raise HTTPException(status_code=500, detail=f"MinIO error: {e}")

# 下载文件从 MinIO
@app.get("/download/{file_name}")
async def download_file(file_name: str):
    ensure_bucket_exists()
    try:
        # 下载文件到本地
        file_path = f"downloaded_{file_name}"
        minio_client.fget_object(BUCKET_NAME, file_name, file_path)

        # 返回文件给客户端
        return {"file_path": file_path}
    except S3Error as e:
        raise HTTPException(status_code=404, detail=f"File '{file_name}' not found.")

# 列出存储桶中的文件
@app.get("/list")
async def list_files():
    ensure_bucket_exists()
    try:
        files = minio_client.list_objects(BUCKET_NAME)
        urls=[]
        for file in files:
            url=minio_client.presigned_get_object(BUCKET_NAME,file.object_name)
            urls.append(url)
        return {"files": urls}
    except S3Error as e:
        raise HTTPException(status_code=500, detail=f"MinIO error: {e}")

# 删除文件
@app.delete("/delete/{file_name}")
async def delete_file(file_name: str):
    ensure_bucket_exists()
    try:
        minio_client.remove_object(BUCKET_NAME, file_name)
        return {"message": f"File '{file_name}' deleted successfully."}
    except S3Error as e:
        raise HTTPException(status_code=404, detail=f"File '{file_name}' not found.")