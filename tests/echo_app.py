import asyncio
import os

from fastapi import FastAPI, Request
import uvicorn


app = FastAPI()

INSTANCE_ID = os.getenv(
    "INSTANCE_ID",
    "unknown",
)


@app.get("/delay/{seconds}")
async def delay(seconds: float):
    await asyncio.sleep(seconds)

    return {
        "upstream": INSTANCE_ID,
        "delay": seconds,
    }


@app.api_route(
    "/{path:path}",
    methods=[
        "GET",
        "POST",
        "PUT",
        "DELETE",
        "HEAD",
    ],
)
async def echo(
    request: Request,
    path: str,
):
    body = await request.body()

    return {
        "upstream": INSTANCE_ID,
        "method": request.method,
        "path": path,
        "headers": dict(request.headers),
        "body": body.decode(errors="replace"),
    }


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=9003, reload=True)
