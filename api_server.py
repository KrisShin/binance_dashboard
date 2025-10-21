# api_server.py (WebSocket 终极版)

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import os
from influxdb_client import InfluxDBClient
from pydantic import BaseModel
from typing import List
import aio_pika # 导入 aio-pika

# --- 配置 (和以前一样) ---
INFLUXDB_URL = "http://influxdb:8086"
INFLUXDB_TOKEN = "my-super-secret-token"
INFLUXDB_ORG = "my-org"
INFLUXDB_BUCKET = "my-bucket"
RABBITMQ_HOST = "rabbitmq"

# --- 应用实例 ---
app = FastAPI(title="实时行情API (WebSocket版)", version="3.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# --- Pydantic 模型 ---
class TradeData(BaseModel):
    time: str
    price: float
    quantity: float
    symbol: str

# --- InfluxDB 客户端 ---
influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
query_api = influx_client.query_api()

# --- 1. (修复) 历史数据API：用于图表初始化 ---
# 我们必须使用聚合，否则前端会崩溃
@app.get("/api/v1/trades/{symbol}", response_model=List[TradeData])
async def get_aggregated_trades(symbol: str, time_range: str = "15m"):
    """
    获取 15 分钟内、按 1 秒聚合的历史数据。
    这是为了解决前端渲染瓶颈，用于快速初始化图表。
    """
    try:
        query_base = f'from(bucket: "{INFLUXDB_BUCKET}")'
        query_range = f'|> range(start: -{time_range})' # 默认 15m
        
        query_filters_and_agg = f'''
              |> filter(fn: (r) => r["_measurement"] == "trade")
              |> filter(fn: (r) => r["symbol"] == "{symbol.upper()}")
              |> filter(fn: (r) => r["_field"] == "price") 
              |> aggregateWindow(every: 1s, fn: mean, createEmpty: false) 
              |> sort(columns: ["_time"], desc: false)
        '''
        flux_query = query_base + query_range + query_filters_and_agg
        tables = query_api.query(flux_query, org=INFLUXDB_ORG)

        results = []
        for table in tables:
            for record in table.records:
                results.append(
                    TradeData(
                        time=record.get_time().isoformat(),
                        price=record.get_value(),
                        quantity=0, # 聚合数据没有quantity
                        symbol=record.values.get("symbol", symbol.upper())
                    )
                )
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Flux query failed: {str(e)}")

# --- 2. (新增) WebSocket 实时推送终结点 ---
@app.websocket("/ws/live")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    
    # 建立与 RabbitMQ 的异步连接
    try:
        connection = await aio_pika.connect_robust(f"amqp://guest:guest@{RABBITMQ_HOST}/")
    except Exception as e:
        await websocket.send_text(f"Error connecting to RabbitMQ: {e}")
        await websocket.close(code=1001)
        return

    async with connection:
        channel = await connection.channel()
        # 声明一个 "fanout" 交换机，它会把消息广播给所有订阅者
        exchange = await channel.declare_exchange(
            "live_trades_broadcast", aio_pika.ExchangeType.FANOUT
        )
        # 声明一个匿名的、排他的队列
        queue = await channel.declare_queue(exclusive=True)
        # 将队列绑定到交换机
        await queue.bind(exchange)

        print("[API Server] WebSocket client connected. Subscribed to live trades.")

        try:
            # 开始从队列消费消息
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        # 消息体是JSON字符串，直接转发给前端
                        await websocket.send_text(message.body.decode('utf-8'))
        
        except WebSocketDisconnect:
            print("[API Server] WebSocket client disconnected.")
        except Exception as e:
            print(f"[API Server] An error occurred in WebSocket: {e}")
        finally:
            # 清理
            await queue.unbind(exchange)
            await queue.delete()
            print("[API Server] Cleaned up WebSocket resources.")


# --- (你其他的 @app.get("/") 路由保持不变) ---
@app.get("/", response_class=FileResponse)
async def read_index():
    index_path = os.path.join(BASE_DIR, "index.html")
    if not os.path.exists(index_path):
        raise HTTPException(status_code=404, detail="index.html not found")
    return FileResponse(index_path)