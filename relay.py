import asyncio
import sys
import websockets

uri = "ws://localhost:6789"

async def send_updates():
    async with websockets.connect(uri) as websocket:
        print(f"Connected to WebSocket server at {uri}")
        while True:
            line = await asyncio.get_event_loop().run_in_executor(None, sys.stdin.readline)
            if not line:
                break
            await websocket.send(line.strip())

if __name__ == "__main__":
    asyncio.run(send_updates())
