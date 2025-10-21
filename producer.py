# producer.py (21并发版)
import asyncio
import websockets
import json
import pika

# --- RabbitMQ 配置 ---
RABBITMQ_HOST = 'rabbitmq'
QUEUE_NAME = 'trades'

# --- 21种热门货币列表 ---
SYMBOLS_TO_SUBSCRIBE = [
    # Top Tiers
    'btcusdt', 'ethusdt', 'solusdt', 'dogeusdt', 'bnbusdt', 'xrpusdt', 'adausdt',
    # DeFi
    'linkusdt', 'uniusdt', 'aaveusdt', 'sushiusdt', 'compusdt', 'mkrusdt', 'grtusdt',
    # L1/L2 & Metaverse
    'avaxusdt', 'dotusdt', 'maticusdt', 'atomusdt', 'sandusdt', 'manausdt', 'shibusdt'
] # 总共 21 个

async def stream_coin(symbol: str, channel):
    """
    一个专门的协程，负责连接一种货币的WebSocket，
    并将其数据发送到RabbitMQ。
    (此函数内容与之前4货币版本完全相同)
    """
    uri = f"wss://stream.binance.com:9443/ws/{symbol}@trade"
    
    while True:
        try:
            async with websockets.connect(uri) as websocket:
                print(f"[Producer] 成功连接到: {symbol}")
                while True:
                    message = await websocket.recv()
                    channel.basic_publish(
                        exchange='',
                        routing_key=QUEUE_NAME,
                        body=message,
                        properties=pika.BasicProperties(
                            delivery_mode=2,
                        ))
        except Exception as e:
            print(f"[Producer] {symbol} 连接断开: {e}. 5秒后重试...")
            await asyncio.sleep(5)

async def main():
    """
    主函数，负责建立RabbitMQ连接，并并发启动所有货币的监听任务
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    print(f"[Producer] RabbitMQ: 已连接并声明队列 '{QUEUE_NAME}'")

    tasks = []
    for symbol in SYMBOLS_TO_SUBSCRIBE:
        tasks.append(stream_coin(symbol, channel))
    
    print(f"[Producer] 即将并发启动 {len(tasks)} 个数据流...")
    # asyncio.gather 会并发运行所有 21 个任务
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[Producer] 程序已停止")