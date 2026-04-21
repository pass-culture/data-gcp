from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger

from app.routes import api

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api)


if __name__ == "__main__":
    import uvicorn

    logger.info("startup", extra={"event": "startup", "response": "ready"})
    uvicorn.run(app, host="0.0.0.0", port=8080)
