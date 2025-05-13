
import requests
import pandas as pd
import json
from datetime import datetime
import time

ALCHEMY_API_KEY = "bI3iF7Ru_cnMnvgl7s7uLcSCbOGomKKB" 


def get_nft_sales(page_key=None, limit=100):
    """从 Alchemy 获取 NFT 销售数据，支持分页"""
    url = f"https://eth-mainnet.g.alchemy.com/nft/v2/{ALCHEMY_API_KEY}/getNFTSales"

    params = {
        "fromBlock": "0",  # 起始区块，0表示从最早开始
        "toBlock": "latest",  # 最新区块
        "order": "desc",  # 降序排列（最新的先显示）
        "marketplace": "seaport",  # 指定 Seaport 协议
        "limit": limit  # 单次请求的最大记录数
    }

    # 添加分页参数
    if page_key:
        params["pageKey"] = page_key

    try:
        print(f"获取 NFT 销售数据 {'(使用页面密钥: ' + page_key + ')' if page_key else ''}...")
        response = requests.get(url, params=params)
        print(f"状态码: {response.status_code}")

        if response.status_code == 200:
            data = response.json()
            if "nftSales" in data:
                print(f"成功! 获取到 {len(data['nftSales'])} 条 NFT 销售记录")
                # 返回销售数据和下一页的页面密钥（如果有）
                return data["nftSales"], data.get("pageKey")
            else:
                print(f"API 返回了意外格式: {data.keys()}")
                return [], None
        else:
            print(f"请求失败: {response.text}")
            return [], None
    except Exception as e:
        print(f"请求出错: {e}")
        return [], None


def get_bulk_nft_sales(max_pages=50, records_per_page=100):
    """获取大量 NFT 销售数据"""
    all_sales = []
    page_key = None
    page_count = 0

    while page_count < max_pages:
        # 获取一页数据
        sales, next_page_key = get_nft_sales(page_key, records_per_page)

        if not sales:
            print("没有获取到更多销售记录")
            break

        # 添加到总列表
        all_sales.extend(sales)
        print(f"已累计获取 {len(all_sales)} 条销售记录")

        # 更新分页密钥
        if not next_page_key:
            print("没有更多页面")
            break

        page_key = next_page_key
        page_count += 1

        # 添加延迟以避免 API 速率限制
        time.sleep(1)

        # 每获取10页保存一次数据，避免数据丢失
        if page_count % 10 == 0:
            print(f"已获取 {page_count} 页数据，临时保存...")
            temp_data = process_sales_data(all_sales)
            save_to_csv(temp_data, f"alchemy_nft_sales_temp_{page_count}.csv")

    print(f"总共获取了 {len(all_sales)} 条销售记录，跨越 {page_count} 页")
    return all_sales


def get_nft_sales_by_date_ranges():
    """通过设置不同的日期范围获取更多历史数据"""
    # 定义多个时间范围（以区块号表示）
    # 这些数字需要根据实际情况调整
    block_ranges = [
        # 最近一个月
        {"fromBlock": "0x1000000", "toBlock": "latest", "name": "recent"},
        # 6个月前
        {"fromBlock": "0xE00000", "toBlock": "0xFFFFFF", "name": "mid_2023"},
        # 1年前
        {"fromBlock": "0xC00000", "toBlock": "0xDFFFFF", "name": "early_2023"},
        # 2年前
        {"fromBlock": "0xA00000", "toBlock": "0xBFFFFF", "name": "late_2022"}
    ]

    all_sales = []

    for block_range in block_ranges:
        print(
            f"\n获取 {block_range['name']} 时期的数据 (区块 {block_range['fromBlock']} 到 {block_range['toBlock']})...")

        url = f"https://eth-mainnet.g.alchemy.com/nft/v2/{ALCHEMY_API_KEY}/getNFTSales"

        params = {
            "fromBlock": block_range["fromBlock"],
            "toBlock": block_range["toBlock"],
            "order": "desc",
            "marketplace": "seaport",
            "limit": 100
        }

        page_key = None
        page_count = 0
        max_pages_per_range = 10  # 每个时间范围最多获取10页

        while page_count < max_pages_per_range:
            # 添加分页参数
            if page_key:
                params["pageKey"] = page_key

            try:
                print(f"获取页面 {page_count + 1}...")
                response = requests.get(url, params=params)

                if response.status_code == 200:
                    data = response.json()
                    if "nftSales" in data:
                        sales = data["nftSales"]
                        if not sales:
                            print("没有销售记录")
                            break

                        print(f"获取到 {len(sales)} 条销售记录")
                        all_sales.extend(sales)

                        # 获取下一页密钥
                        page_key = data.get("pageKey")
                        if not page_key:
                            print("没有更多页面")
                            break
                    else:
                        print(f"API 返回了意外格式: {data.keys()}")
                        break
                else:
                    print(f"请求失败: {response.status_code} - {response.text}")
                    break

                page_count += 1
                time.sleep(1)  # 添加延迟

            except Exception as e:
                print(f"获取数据时出错: {e}")
                break

        print(f"从 {block_range['name']} 时期获取了 {page_count} 页数据")

        # 每个时间范围后保存一次临时数据
        if all_sales:
            temp_data = process_sales_data(all_sales)
            save_to_csv(temp_data, f"alchemy_nft_sales_{block_range['name']}.csv")

    print(f"总共从所有时间范围获取了 {len(all_sales)} 条销售记录")
    return all_sales


def process_sales_data(sales):
    """处理销售数据为结构化格式"""
    processed_data = []

    for sale in sales:
        try:
            # 提取基本销售信息
            sale_data = {
                "marketplace": sale.get("marketplace", ""),
                "contract_address": sale.get("contractAddress", ""),
                "token_id": sale.get("tokenId", ""),
                "quantity": sale.get("quantity", "1"),
                "buyer_address": sale.get("buyerAddress", ""),
                "seller_address": sale.get("sellerAddress", ""),
                "taker": sale.get("taker", ""),
                "transaction_hash": sale.get("transactionHash", ""),
                "block_number": int(sale.get("blockNumber", "0"), 16) if isinstance(sale.get("blockNumber"),
                                                                                    str) else sale.get("blockNumber",
                                                                                                       0),
                "block_timestamp": sale.get("blockTimestamp", "")
            }

            # 提取价格信息
            if "sellerFee" in sale:
                seller_fee = sale["sellerFee"]
                sale_data["price_token"] = seller_fee.get("symbol", "")
                sale_data["price_amount"] = float(seller_fee.get("amount", "0"))

                # 如果是 ETH，转换为 ETH 单位
                if seller_fee.get("symbol") == "ETH":
                    sale_data["price_eth"] = float(seller_fee.get("amount", "0")) / 1e18
                else:
                    sale_data["price_eth"] = 0

            # 添加额外的 NFT 元数据（如果有）
            if "nft" in sale:
                nft_data = sale["nft"]
                sale_data["nft_title"] = nft_data.get("title", "")
                sale_data["nft_description"] = nft_data.get("description", "")

                # 安全提取图片URL
                if "image" in nft_data and isinstance(nft_data["image"], dict):
                    sale_data["nft_image_url"] = nft_data["image"].get("originalUrl", "")
                else:
                    sale_data["nft_image_url"] = ""

                # 添加集合信息
                if "collection" in nft_data and isinstance(nft_data["collection"], dict):
                    sale_data["collection_name"] = nft_data["collection"].get("name", "")
                    sale_data["collection_slug"] = nft_data["collection"].get("slug", "")
                else:
                    sale_data["collection_name"] = ""
                    sale_data["collection_slug"] = ""

            processed_data.append(sale_data)
        except Exception as e:
            print(f"处理销售记录时出错: {e}")
            continue

    return processed_data


def save_to_csv(data, filename="nft_sales_data.csv"):
    """将处理后的数据保存为 CSV"""
    if not data:
        print("没有数据可保存")
        return

    try:
        # 创建 DataFrame
        df = pd.DataFrame(data)

        # 添加时间相关列
        if "block_timestamp" in df.columns:
            df["datetime"] = pd.to_datetime(df["block_timestamp"])
            df["date"] = df["datetime"].dt.date
            df["time"] = df["datetime"].dt.time

        # 保存 CSV
        df.to_csv(filename, index=False)

        # 显示数据统计
        print(f"\n成功保存 {len(df)} 条 NFT 销售记录到 {filename}")
        print("\n数据统计:")
        print(f"总销售数: {len(df)}")

        if len(df) > 0:
            if "price_eth" in df.columns:
                eth_sales = df[df["price_eth"] > 0]
                if not eth_sales.empty:
                    print(f"ETH 交易数量: {len(eth_sales)}")
                    print(f"平均价格: {eth_sales['price_eth'].mean():.4f} ETH")
                    print(f"最高价格: {eth_sales['price_eth'].max():.4f} ETH")
                    print(f"最低价格: {eth_sales['price_eth'].min():.4f} ETH")

            if "collection_name" in df.columns:
                top_collections = df["collection_name"].value_counts().head(5)
                print("\n热门 NFT 集合:")
                for collection, count in top_collections.items():
                    if collection:  # 只显示非空集合名称
                        print(f"- {collection}: {count} 销售")

            if "datetime" in df.columns:
                print(f"\n时间范围: {df['datetime'].min()} 到 {df['datetime'].max()}")

        return df
    except Exception as e:
        print(f"保存数据时出错: {e}")
        return None


def merge_csv_files(file_pattern="alchemy_nft_sales_*.csv", output_file="all_nft_sales.csv"):
    """合并多个CSV文件"""
    import glob

    try:
        print(f"查找匹配 '{file_pattern}' 的CSV文件...")
        csv_files = glob.glob(file_pattern)

        if not csv_files:
            print("未找到匹配的CSV文件")
            return

        print(f"找到 {len(csv_files)} 个CSV文件")

        # 读取所有CSV文件
        all_dfs = []
        for file in csv_files:
            print(f"读取 {file}...")
            df = pd.read_csv(file)
            print(f"- 包含 {len(df)} 条记录")
            all_dfs.append(df)

        # 合并所有DataFrame
        merged_df = pd.concat(all_dfs, ignore_index=True)

        # 删除重复记录
        before_dedup = len(merged_df)
        merged_df = merged_df.drop_duplicates(subset=["transaction_hash", "token_id"], keep="first")
        after_dedup = len(merged_df)

        print(f"合并了 {len(all_dfs)} 个CSV文件")
        print(f"删除了 {before_dedup - after_dedup} 条重复记录")

        # 保存合并后的CSV
        merged_df.to_csv(output_file, index=False)
        print(f"合并后的数据已保存到 {output_file}，共 {len(merged_df)} 条记录")

        return merged_df
    except Exception as e:
        print(f"合并CSV文件时出错: {e}")
        return None


def analyze_nft_sales(df=None, filename=None):
    """分析 NFT 销售数据"""
    try:
        # 如果没有提供DataFrame，从文件读取
        if df is None and filename:
            df = pd.read_csv(filename)

        if df is None:
            print("没有提供数据进行分析")
            return None

        print(f"分析 {len(df)} 条 NFT 销售记录...")

        # 确保日期时间格式正确
        if "block_timestamp" in df.columns:
            df["datetime"] = pd.to_datetime(df["block_timestamp"])
            df["date"] = df["datetime"].dt.date

        analysis_results = {}

        # 1. 每日销售量分析
        if "date" in df.columns:
            daily_sales = df.groupby("date").size().reset_index(name="sales_count")
            analysis_results["daily_sales"] = daily_sales
            print(f"\n分析了 {len(daily_sales)} 天的销售数据")

        # 2. 价格分析
        if "price_eth" in df.columns:
            # 过滤有效价格
            valid_prices = df[df["price_eth"] > 0]["price_eth"]

            price_stats = {
                "min": valid_prices.min(),
                "max": valid_prices.max(),
                "mean": valid_prices.mean(),
                "median": valid_prices.median(),
                "std": valid_prices.std()
            }

            analysis_results["price_stats"] = price_stats
            print("\n价格统计 (ETH):")
            for stat, value in price_stats.items():
                print(f"- {stat}: {value:.4f}")

            # 价格区间分布
            bins = [0, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, float('inf')]
            labels = ["<0.01", "0.01-0.05", "0.05-0.1", "0.1-0.5", "0.5-1", "1-5", "5-10", ">10"]
            df["price_range"] = pd.cut(df["price_eth"], bins=bins, labels=labels)
            price_ranges = df["price_range"].value_counts().sort_index().reset_index()
            price_ranges.columns = ["price_range", "sales_count"]

            analysis_results["price_ranges"] = price_ranges
            print("\n价格区间分布:")
            for _, row in price_ranges.iterrows():
                print(f"- {row['price_range']}: {row['sales_count']} 销售")

        # 3. 集合分析
        if "collection_name" in df.columns:
            # 过滤有效集合名称
            df_valid_collections = df[df["collection_name"].notna() & (df["collection_name"] != "")]

            if not df_valid_collections.empty:
                top_collections = df_valid_collections["collection_name"].value_counts().head(20).reset_index()
                top_collections.columns = ["collection", "sales_count"]

                analysis_results["top_collections"] = top_collections
                print("\n热门 NFT 集合 (前10):")
                for _, row in top_collections.head(10).iterrows():
                    print(f"- {row['collection']}: {row['sales_count']} 销售")

                # 集合价格分析
                collection_prices = df_valid_collections.groupby("collection_name")["price_eth"].agg(
                    ['count', 'mean', 'median', 'min', 'max']).reset_index()
                collection_prices = collection_prices.sort_values(by="count", ascending=False).head(10)

                analysis_results["collection_prices"] = collection_prices
                print("\n热门集合价格分析:")
                for _, row in collection_prices.iterrows():
                    print(f"- {row['collection_name']}: 平均 {row['mean']:.4f} ETH, 中位数 {row['median']:.4f} ETH")

        # 4. 活跃度分析
        if "buyer_address" in df.columns and "seller_address" in df.columns:
            top_buyers = df["buyer_address"].value_counts().head(10).reset_index()
            top_buyers.columns = ["address", "purchase_count"]

            top_sellers = df["seller_address"].value_counts().head(10).reset_index()
            top_sellers.columns = ["address", "sales_count"]

            analysis_results["top_buyers"] = top_buyers
            analysis_results["top_sellers"] = top_sellers

            print("\n活跃买家/卖家分析完成")

        # 5. 时间趋势分析
        if "datetime" in df.columns and "price_eth" in df.columns:
            # 月度趋势
            df["month"] = df["datetime"].dt.to_period("M")
            monthly_trends = df.groupby("month").agg({
                "price_eth": ["count", "mean", "median", "sum"],
                "transaction_hash": "nunique"
            }).reset_index()
            monthly_trends.columns = ["month", "sales_count", "avg_price", "median_price", "volume", "unique_txns"]

            analysis_results["monthly_trends"] = monthly_trends
            print("\n月度趋势分析完成")

        # 保存分析结果
        print("\n保存分析结果...")

        # 保存汇总报告
        with open("nft_analysis_report.txt", "w") as f:
            f.write("NFT 销售数据分析报告\n")
            f.write("======================\n\n")

            f.write(f"分析记录总数: {len(df)}\n")

            if "datetime" in df.columns:
                f.write(f"数据时间范围: {df['datetime'].min()} 到 {df['datetime'].max()}\n\n")

            if "price_stats" in analysis_results:
                f.write("价格统计 (ETH):\n")
                for stat, value in analysis_results["price_stats"].items():
                    f.write(f"- {stat}: {value:.4f}\n")
                f.write("\n")

            if "price_ranges" in analysis_results:
                f.write("价格区间分布:\n")
                for _, row in analysis_results["price_ranges"].iterrows():
                    f.write(f"- {row['price_range']}: {row['sales_count']} 销售\n")
                f.write("\n")

            if "top_collections" in analysis_results:
                f.write("热门 NFT 集合 (前10):\n")
                for _, row in analysis_results["top_collections"].head(10).iterrows():
                    f.write(f"- {row['collection']}: {row['sales_count']} 销售\n")
                f.write("\n")

        print("分析报告已保存到 nft_analysis_report.txt")

        # 保存各项分析结果为CSV
        for name, data in analysis_results.items():
            if isinstance(data, pd.DataFrame):
                data.to_csv(f"analysis_{name}.csv", index=False)
                print(f"- {name} 分析结果已保存到 analysis_{name}.csv")

        return analysis_results

    except Exception as e:
        print(f"分析数据时出错: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    print("开始使用 Alchemy API 获取 NFT 销售数据...\n")

    # 选择数据获取方法
    print("选择数据获取方法:")
    print("1. 使用分页获取大量数据 (单一时间段)")
    print("2. 使用多个时间范围获取更全面的历史数据")
    print("3. 合并已有的CSV文件并分析")

    choice = input("请输入选择 (1/2/3): ")

    if choice == "1":
        # 获取用户输入的页数
        try:
            max_pages = int(input("要获取的最大页数 (建议50-200): ") or "50")
        except ValueError:
            max_pages = 50
            print(f"使用默认值: {max_pages}页")

        # 获取销售数据
        all_sales = get_bulk_nft_sales(max_pages=max_pages)

        if all_sales:
            # 处理销售数据
            processed_data = process_sales_data(all_sales)

            # 保存数据
            df = save_to_csv(processed_data, "alchemy_bulk_nft_sales.csv")

            # 分析数据
            if df is not None:
                analyze_nft_sales(df=df)

    elif choice == "2":
        # 使用多个时间范围获取数据
        all_sales = get_nft_sales_by_date_ranges()

        if all_sales:
            # 处理销售数据
            processed_data = process_sales_data(all_sales)

            # 保存数据
            df = save_to_csv(processed_data, "alchemy_historical_nft_sales.csv")

            # 分析数据
            if df is not None:
                analyze_nft_sales(df=df)

        # 合并所有生成的CSV文件
        merge_csv_files()

    elif choice == "3":
        # 合并现有CSV文件
        merged_df = merge_csv_files()

        # 分析合并后的数据
        if merged_df is not None:
            analyze_nft_sales(df=merged_df)

    else:
        print("无效选择，退出程序")