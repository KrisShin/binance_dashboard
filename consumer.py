# consumer.py (批量异步写入 修复版)
import pika
import json
from influxdb_client import InfluxDBClient, Point
# 我们不再需要 SYNCHRONOUS
# from influxdb_client.client.write_api import SYNCHRONOUS
import atexit # 用于注册一个退出处理函数

# --- 配置 (保持不变) ---
INFLUXDB_URL = "http://influxdb:8086"
INFLUXDB_TOKEN = "my-super-secret-token"
INFLUXDB_ORG = "my-org"
INFLUXDB_BUCKET = "my-bucket"
RABBITMQ_HOST = 'rabbitmq'
QUEUE_NAME = 'trades'
BROADCAST_EXCHANGE_NAME = 'live_trades_broadcast'

def main():
    # --- InfluxDB Client ---
    influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    
    # --- 核心修复 1: 使用默认的异步批量写入API ---
    # 我们删除了 'write_options=SYNCHRONOUS'
    # 客户端现在会自动在后台批量处理数据
    # batch_size=500, flush_interval=1000 (默认值)
    write_api = influx_client.write_api()
    print("[Consumer] InfluxDB: 已成功连接 (已启用异步批量写入模式)")

    # --- 核心修复 2: 注册一个退出处理器 ---
    # 当容器停止时 (Ctrl+C 或 docker-compose down),
    # 我们需要确保缓冲区里最后的数据被成功写入
    def on_exit():
        print("[Consumer] 正在关闭... 强制刷新 InfluxDB 缓冲区...")
        write_api.close() # 刷新所有剩余的写入
        influx_client.close()
        print("[Consumer] InfluxDB 缓冲区已刷新。退出。")
    
    atexit.register(on_exit)

    # --- RabbitMQ Connection (双通道, 保持不变) ---
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    
    channel_publish = connection.channel()
    channel_publish.exchange_declare(
        exchange=BROADCAST_EXCHANGE_NAME, 
        exchange_type='fanout'
    )
    
    channel_consume = connection.channel()
    channel_consume.queue_declare(queue=QUEUE_NAME, durable=True)
    # (新增) 设置 QoS，告诉 RabbitMQ 一次只给我 1000 条消息
    # 在我 ack 之前，不要给我更多。这有助于防止 consumer 内存也爆炸
    channel_consume.basic_qos(prefetch_count=1000)

    print(f"[Consumer] RabbitMQ: 已连接并设置 QoS (prefetch=1000)")
    print('[Consumer] [*] 等待消息中...')

    def callback(ch, method, properties, body):
        try:
            # 1. (不变) 广播
            channel_publish.basic_publish(
                exchange=BROADCAST_EXCHANGE_NAME,
                routing_key='',
                body=body
            )
            
            # 2. (不变) 解析
            message_str = body.decode('utf-8')
            data = json.loads(message_str)
            point = Point("trade") \
                .tag("symbol", data['s']) \
                .field("price", float(data['p'])) \
                .field("quantity", float(data['q'])) \
                .time(int(data['T']), "ms")

            # 3. (核心修复 3) 写入缓冲区
            # 这个调用现在是“非阻塞”的，它会立刻返回
            write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
            
        except Exception as e:
            print(f"[Consumer] 处理消息时发生错误: {e}")
        finally:
            # 4. (核心修复 4) 立即 ACK
            # 因为 'write_api.write' 已经把数据接管到缓冲区了，
            # 我们可以（也必须）立即 ACK 消息，告诉 RabbitMQ 我们处理完了
            ch.basic_ack(delivery_tag=method.delivery_tag)

    channel_consume.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)
    
    try:
        channel_consume.start_consuming()
    except KeyboardInterrupt:
        print('[Consumer] 程序已中断')
    except pika.exceptions.ConnectionClosedByBroker:
        print("[Consumer] RabbitMQ 连接被代理关闭")
    finally:
        if connection.is_open:
            connection.close()
        print("[Consumer] RabbitMQ 连接已关闭")

if __name__ == '__main__':
    main()