import asyncio
import websockets

connected = set()

async def handler(websocket):
    # Register.
    connected.add(websocket)
    try:
        async for message in websocket:
            for conn in connected:
                if conn != websocket:
                    await conn.send(message)
    except websockets.ConnectionClosed:
        pass
    finally:
        connected.remove(websocket)

async def main():
    async with websockets.serve(handler, "0.0.0.0", 6789):
        print("WebSocket server started on ws://0.0.0.0:6789")
        await asyncio.Future()  # run forever

if __name__ == "__main__":
    asyncio.run(main())
