# api_server.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from influxdb_client import InfluxDBClient
from pydantic import BaseModel
from typing import List

# --- InfluxDB 配置 ---
# (和你的consumer.py中的配置保持一致)
INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = "my-super-secret-token"
INFLUXDB_ORG = "my-org"
INFLUXDB_BUCKET = "my-bucket"

# --- FastAPI 应用实例 ---
app = FastAPI(title="实时行情API", description="一个用于查询实时交易数据的API", version="1.0.0")

# --- CORS 中间件配置 ---
# 允许所有来源的跨域请求，方便前端开发
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- 数据模型 (Pydantic) ---
# 定义API返回的数据结构，FastAPI会自动处理数据验证和JSON序列化
class TradeData(BaseModel):
    time: str
    price: float
    quantity: float
    symbol: str


# --- InfluxDB Client 实例 ---
# 在应用启动时创建，以便复用
influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
query_api = influx_client.query_api()


# --- API Endpoint ---
@app.get("/api/v1/trades/{symbol}", response_model=List[TradeData])
async def get_trades(symbol: str, time_range: str = "1h"):
    """
    获取指定交易对在特定时间范围内的交易数据。

    - **symbol**: 交易对, 例如 'BTCUSDT'.
    - **time_range**: 时间范围, 例如 '1h', '15m', '1d'.
    """
    try:
        # --- Flux 查询语句 ---
        # 这是InfluxDB的查询语言，非常强大
        flux_query = f'''
            from(bucket: "{INFLUXDB_BUCKET}")
              |> range(start: -{time_range})
              |> filter(fn: (r) => r["_measurement"] == "trade")
              |> filter(fn: (r) => r["symbol"] == "{symbol.upper()}")
              |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
              |> keep(columns: ["_time", "price", "quantity", "symbol"])
        '''

        # --- 执行查询 ---
        tables = query_api.query(flux_query, org=INFLUXDB_ORG)

        # --- 格式化结果 ---
        results = []
        for table in tables:
            for record in table.records:
                results.append(
                    TradeData(
                        time=record.get_time().isoformat(),
                        price=record['price'],
                        quantity=record['quantity'],
                        symbol=record['symbol'],
                    )
                )

        return results

    except Exception as e:
        # 如果发生错误，返回一个HTTP 500错误
        raise HTTPException(status_code=500, detail=str(e))


# --- 应用根路径 ---
@app.get("/")
def read_root():
    return {"message": "欢迎来到实时行情API, 请访问 /docs 查看API文档"}
