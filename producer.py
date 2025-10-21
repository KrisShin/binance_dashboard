# producer.py
import asyncio
import websockets
import pika

# --- RabbitMQ 配置 ---
RABBITMQ_HOST = 'rabbitmq'
QUEUE_NAME = 'trades'

# 建立到RabbitMQ的连接
connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
channel = connection.channel()

# 声明一个队列，如果队列不存在，则会被创建。durable=True意味着队列在RabbitMQ重启后依然存在
channel.queue_declare(queue=QUEUE_NAME, durable=True)
print(f"RabbitMQ: 已连接并声明队列 '{QUEUE_NAME}'")


# --- Binance WebSocket 配置 ---
BINANCE_URI = "wss://stream.binance.com:9443/ws/btcusdt@trade"


async def binance_producer():
    """连接到币安，接收数据并发送到RabbitMQ"""
    async with websockets.connect(BINANCE_URI) as websocket:
        print(f"成功连接到: {BINANCE_URI}")
        while True:
            message = await websocket.recv()
            # 将收到的原始消息（字符串）直接发送到队列
            channel.basic_publish(
                exchange='',  # 使用默认交换机
                routing_key=QUEUE_NAME,  # 路由键就是队列名
                body=message,  # 消息内容
                properties=pika.BasicProperties(
                    delivery_mode=2,  # 让消息持久化
                ),
            )
            # (可选) 可以在这里打印一下，确认生产者在工作
            # print(f" [x] 已发送消息到RabbitMQ")


if __name__ == "__main__":
    try:
        asyncio.run(binance_producer())
    except KeyboardInterrupt:
        print("程序已停止")
        connection.close()
