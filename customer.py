# consumer.py
import pika
import json

# --- RabbitMQ 配置 ---
RABBITMQ_HOST = 'localhost'
RABBITMQ_PORT = '7672'
QUEUE_NAME = 'trades'

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT))
    channel = connection.channel()

    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    print(' [*] 等待消息中... 按 CTRL+C 退出')

    def callback(ch, method, properties, body):
        """收到消息后要执行的回调函数"""
        # body是二进制格式，我们先解码成字符串
        message_str = body.decode('utf-8')
        # 解析JSON数据
        data = json.loads(message_str)
        price = float(data['p'])
        quantity = float(data['q'])

        print(f" [x] 已接收: 价格={price:.2f}, 数量={quantity:.6f}")
        
        # 确认消息已被处理，RabbitMQ可以把它删掉了
        ch.basic_ack(delivery_tag=method.delivery_tag)

    # 告诉RabbitMQ这个回调函数将从'trades'队列接收消息
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)

    # 进入一个无限循环，等待数据并调用回调函数
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('程序已中断')