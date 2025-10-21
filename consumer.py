# consumer.py (升级版)
import pika
import json
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# --- InfluxDB 配置 ---
INFLUXDB_URL = "http://influxdb:8086"
INFLUXDB_TOKEN = "my-super-secret-token"  # 这是你在docker命令中设置的TOKEN
INFLUXDB_ORG = "my-org"
INFLUXDB_BUCKET = "my-bucket"

# --- RabbitMQ 配置 ---
RABBITMQ_HOST = 'rabbitmq'
QUEUE_NAME = 'trades'


def main():
    # --- 初始化 InfluxDB Client ---
    # 在这里创建客户端实例，以便在回调中复用
    influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    # 获取写入API，SYNCHRONOUS表示同步写入，确保每条消息都成功写入
    write_api = influx_client.write_api(write_options=SYNCHRONOUS)
    print("InfluxDB: 已成功连接")

    # --- 初始化 RabbitMQ Connection ---
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    print(' [*] 等待消息中... 按 CTRL+C 退出')

    def callback(ch, method, properties, body):
        """收到消息后，解析并写入InfluxDB"""
        try:
            message_str = body.decode('utf-8')
            data = json.loads(message_str)

            # --- 创建一个数据点 (Point) ---
            # 这是InfluxDB的数据格式
            point = (
                Point("trade")
                .tag("symbol", data['s'])
                .field("price", float(data['p']))
                .field("quantity", float(data['q']))
                .time(int(data['T']), "ms")
            )  # 使用交易时间作为数据点的时间戳

            # --- 将数据点写入 InfluxDB ---
            write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)

            # (可选) 在终端打印确认信息
            print(f" [x] 已接收并写入InfluxDB: 价格={float(data['p']):.2f}")

        except Exception as e:
            print(f"处理消息时发生错误: {e}")
        finally:
            # 无论成功与否，都确认消息，避免队列阻塞
            ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('程序已中断')
