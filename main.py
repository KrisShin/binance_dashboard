import asyncio
import websockets
import json

# 这是币安BTC/USDT交易对的WebSocket地址
# 你可以把 'btcusdt' 换成 'ethusdt' 等其他交易对
URI = "wss://stream.binance.com:9443/ws/btcusdt@trade"


async def recv_data():
    """
    连接到WebSocket并持续接收数据
    """
    # 使用 async with 来自动管理连接的建立和关闭
    async with websockets.connect(URI) as websocket:
        print(f"成功连接到: {URI}")
        while True:
            try:
                # 等待并接收来自服务器的消息
                message = await websocket.recv()

                # 收到的消息是JSON字符串，我们把它解析成Python字典
                data = json.loads(message)

                # 提取我们关心的信息：价格(p) 和 数量(q)
                price = float(data['p'])
                quantity = float(data['q'])
                trade_time = data['T']  # 交易时间戳

                # 在终端打印出来
                print(f"时间: {trade_time} | 价格: {price:.2f} | 数量: {quantity:.6f}")

            except websockets.exceptions.ConnectionClosed as e:
                print(f"连接已关闭: {e}")
                break
            except Exception as e:
                print(f"发生错误: {e}")
                break


if __name__ == "__main__":
    print("正在启动数据接收客户端...")
    # 运行异步函数
    asyncio.run(recv_data())
