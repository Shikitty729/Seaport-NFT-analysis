

import requests       # 发送 HTTP 请求
import pandas as pd  # DataFrame 操作
import csv            # CSV 读写
import argparse      # 命令行参数解析

# —————— 复杂度分析 ——————
# 时间复杂度：O(N) 次网络调用，总记录数 N，每次拉 batch_size 条 → N/batch_size 次请求
# 空间复杂度：
#   • 内存方案：O(N)，所有数据累积在内存里
#   • 流式方案：O(batch_size)，只保存当前批次数据

# —————— 全局配置 ——————
API_KEY = "f360900236515fe76f0a5d11604e3a6c"  # The Graph API 密钥
API_URL = (
    f"https://gateway.thegraph.com/api/{API_KEY}"
    "/subgraphs/id/2GmLsgYGWoFoouZzKjp8biYDkfmeLTkEY3VDQyZqSJHA"
)  # Seaport 子图 GraphQL 端点

def fetch_nft_sales(limit: int, skip: int) -> list:
    """
    从 GraphQL 分页拉取一页 orderFulfillments。
    :param limit: 本页最大记录数
    :param skip:  偏移量（已跳过的记录数）
    :return:      包含若干 dict，每个 dict 结构如：
                  {
                    "id": "...",
                    "orderFulfillmentMethod": "...",
                    "trade": { "id": "...", 
                               "timestamp": ..., 
                               "priceETH": "...", 
                               "tokenId": "...", 
                               "buyer": "...", 
                               "seller": "..."
                              }
                  }
    """
    query = """
    query GetNFTSales($first: Int!, $skip: Int!) {
      orderFulfillments(first: $first, skip: $skip) {
        id
        orderFulfillmentMethod
        trade {
          id
          timestamp
          priceETH
          tokenId
          buyer
          seller
        }
      }
    }
    """
    resp = requests.post(API_URL, json={"query": query, "variables": {"first": limit, "skip": skip}})
    resp.raise_for_status()
    js = resp.json()
    if js.get("errors"):
        # 如果 GraphQL 返回错误，抛出并显示详细信息
        raise RuntimeError(f"GraphQL Error: {js['errors']}")
    return (js.get("data") or {}).get("orderFulfillments", [])

def fetch_all_to_memory(batch_size: int = 1000) -> pd.DataFrame:
    """
    内存方案：循环分页拉取，全部累积到列表，再转换成 DataFrame 并返回。
    :param batch_size: 每页大小
    :return:           DataFrame，列包括:
                        ['id','orderFulfillmentMethod',
                         'trade.id','trade.timestamp','trade.priceETH',
                         'trade.tokenId','trade.buyer','trade.seller']
    """
    all_records = []
    skip = 0
    while True:
        page = fetch_nft_sales(limit=batch_size, skip=skip)
        if not page:
            break
        all_records.extend(page)
        skip += len(page)
    # 利用 pandas.json_normalize 展开嵌套 dict
    df = pd.json_normalize(all_records)
    # 把 UNIX 秒转为 datetime
    df["trade.timestamp"] = pd.to_datetime(df["trade.timestamp"], unit="s")
    return df

def stream_all_to_csv(batch_size: int = 1000, filename: str = "all_sales_stream.csv"):
    """
    流式方案：一边拉一边写 CSV，内存只保留当前批次。
    :param batch_size: 每页大小
    :param filename:   输出的 CSV 文件名
    """
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        # 先写表头
        writer.writerow([
            "fulfillment_id","method",
            "trade_id","timestamp","priceETH","tokenId","buyer","seller"
        ])
        skip = 0
        while True:
            page = fetch_nft_sales(limit=batch_size, skip=skip)
            if not page:
                break
            for rec in page:
                t = rec["trade"]
                writer.writerow([
                    rec["id"],
                    rec["orderFulfillmentMethod"],
                    t.get("id"),
                    pd.to_datetime(t.get("timestamp"), unit="s"),
                    t.get("priceETH"),
                    t.get("tokenId"),
                    t.get("buyer"),
                    t.get("seller"),
                ])
            skip += len(page)

def main():
    parser = argparse.ArgumentParser(description="下载 Seaport 全量 NFT 销售数据")
    parser.add_argument("--mode", choices=["memory","stream","all"], default="all",
                        help="运行模式：memory=内存方案；stream=流式方案；all=两个都跑")
    parser.add_argument("--batch", type=int, default=1000, help="每页拉取记录数")
    parser.add_argument("--out-memory", default="all_sales_memory.csv",
                        help="内存方案输出文件名")
    parser.add_argument("--out-stream", default="all_sales_stream.csv",
                        help="流式方案输出文件名")
    args = parser.parse_args()

    if args.mode in ("memory","all"):
        df = fetch_all_to_memory(batch_size=args.batch)
        df.to_csv(args.out_memory, index=False)
        print(f"[内存方案] 拉取 {len(df)} 条，已写入 {args.out_memory}")

    if args.mode in ("stream","all"):
        stream_all_to_csv(batch_size=args.batch, filename=args.out_stream)
        print(f"[流式方案] 已写入 {args.out_stream}")

if __name__ == "__main__":
    main()

