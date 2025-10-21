# consumer.py (双通道修复版)
import pika
import json
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# --- 配置 (保持不变) ---
INFLUXDB_URL = "http://influxdb:8086"
INFLUXDB_TOKEN = "my-super-secret-token"
INFLUXDB_ORG = "my-org"
INFLUXDB_BUCKET = "my-bucket"
RABBITMQ_HOST = 'rabbitmq'
QUEUE_NAME = 'trades'
BROADCAST_EXCHANGE_NAME = 'live_trades_broadcast'

def main():
    # --- InfluxDB Client (保持不变) ---
    influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    write_api = influx_client.write_api(write_options=SYNCHRONOUS)
    print("[Consumer] InfluxDB: 已成功连接")

    # --- RabbitMQ Connection (保持不变) ---
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))

    # --- 核心修复：创建两个独立的通道 ---
    
    # 1. 创建用于“发布”广播的通道
    channel_publish = connection.channel()
    channel_publish.exchange_declare(
        exchange=BROADCAST_EXCHANGE_NAME, 
        exchange_type='fanout'
    )
    print(f"[Consumer] RabbitMQ: 已创建用于广播的 Channel 1 和 '{BROADCAST_EXCHANGE_NAME}' 交换机")

    # 2. 创建用于“消费”数据的通道
    channel_consume = connection.channel()
    channel_consume.queue_declare(queue=QUEUE_NAME, durable=True)
    print(f"[Consumer] RabbitMQ: 已创建用于消费的 Channel 2 和 '{QUEUE_NAME}' 队列")

    print('[Consumer] [*] 等待消息中...')

    def callback(ch, method, properties, body):
        """
        这个回调函数现在只使用 'channel_publish' 来发送
        并使用 'ch' (即 channel_consume) 来确认消息
        """
        try:
            # --- 1. (修复) 使用独立的 'channel_publish' 来广播 ---
            channel_publish.basic_publish(
                exchange=BROADCAST_EXCHANGE_NAME,
                routing_key='', # fanout 交换机忽略 routing_key
                body=body
            )
            
            # --- 2. (不变) 解析并写入 InfluxDB ---
            message_str = body.decode('utf-8')
            data = json.loads(message_str)

            point = Point("trade") \
                .tag("symbol", data['s']) \
                .field("price", float(data['p'])) \
                .field("quantity", float(data['q'])) \
                .time(int(data['T']), "ms")

            write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
            
        except Exception as e:
            print(f"[Consumer] 处理消息时发生错误: {e}")
        finally:
            # --- 3. (不变) 使用 'ch' (channel_consume) 来确认 ---
            ch.basic_ack(delivery_tag=method.delivery_tag)

    # (不变) 告诉 'channel_consume' 去消费
    channel_consume.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)
    
    try:
        # (不变) 启动 'channel_consume' 的消费循环
        channel_consume.start_consuming()
    except KeyboardInterrupt:
        print('程序已中断')
    finally:
        connection.close()
        print("[Consumer] RabbitMQ 连接已关闭")

if __name__ == '__main__':
    main()