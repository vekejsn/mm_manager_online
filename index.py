from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
import uvicorn

app = FastAPI()

from routers import terminals_info

app.include_router(terminals_info.router, prefix="/api/info", tags=["info"])

app.mount("/", StaticFiles(directory="static",html = True), name="static")

if __name__ == "__main__":
    uvicorn.run(app, port=27273)