import uvicorn
import os

def start_server(host="0.0.0.0",
                 port=9999,
                 num_workers=1,
                 loop="asyncio",
                 reload=True):
    uvicorn.run("main:app",
                host=host,
                port=port,
                workers=num_workers,
                loop=loop,
                reload=reload)


if __name__ == "__main__":
    start_server()