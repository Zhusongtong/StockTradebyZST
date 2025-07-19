from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import random
import sys
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional

import pandas as pd
import requests
from mootdx.quotes import Quotes
from tqdm import tqdm

warnings.filterwarnings("ignore")

# --------------------------- 全局日志配置 --------------------------- #
LOG_FILE = Path("fetch.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
    ],
)
logger = logging.getLogger("fetch_kline")

# 屏蔽第三方库多余 INFO 日志
for noisy in ("httpx", "urllib3", "_client"):
    logging.getLogger(noisy).setLevel(logging.WARNING)

# --------------------------- 市值快照 (数据源: 东方财富 API) --------------------------- #

def _get_mktcap_eastmoney() -> pd.DataFrame:
    """
    通过直接调用东方财富API获取A股实时市值快照.
    返回列: code, mktcap (单位: 元)
    """
    logger.info("正在通过东方财富API获取全市场市值快照...")
    api_url = "http://82.push2.eastmoney.com/api/qt/clist/get"
    fields = "f12,f14,f20"  # f12:代码, f14:名称, f20:总市值
    params = {
        "pn": 1,  # Page number
        "pz": 5000,  # Page size
        "po": 1,
        "np": 1,
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": 2,
        "invt": 2,
        "fid": "f3",
        "fs": "m:0+t:6,m:0+t:13,m:1+t:2,m:1+t:23",  # A股市场
        "fields": fields,
        "_": int(time.time() * 1000),
    }

    for attempt in range(1, 4):
        try:
            response = requests.get(api_url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            if not data["data"] or not data["data"]["diff"]:
                logger.warning("东方财富API未返回有效数据 (第%d次尝试)", attempt)
                time.sleep(3 * attempt)
                continue

            df = pd.DataFrame(data["data"]["diff"])
            df = df.rename(columns={"f12": "code", "f20": "mktcap"})
            # API返回的市值可能为'-'，需要处理
            df["mktcap"] = pd.to_numeric(df["mktcap"], errors="coerce")
            df = df.dropna(subset=["code", "mktcap"])
            df = df[["code", "mktcap"]]
            
            logger.info("成功获取 %d 只股票的市值快照。", len(df))
            return df

        except Exception as e:
            logger.warning("东方财富API获取市值快照失败(%d/3): %s", attempt, e)
            time.sleep(random.uniform(3, 5) * attempt)
    
    raise RuntimeError("东方财富API连续三次拉取市值快照失败！")


# --------------------------- 股票池筛选 --------------------------- #

def get_constituents(
    min_cap: float,
    max_cap: float,
    exclude_gem_etc: bool,
    mktcap_df: Optional[pd.DataFrame] = None,
) -> List[str]:
    """根据市值和板块筛选股票池"""
    df = mktcap_df if mktcap_df is not None else _get_mktcap_eastmoney()

    # 市值单位为元
    cond = (df["mktcap"] >= min_cap) & (df["mktcap"] <= max_cap)
    
    if exclude_gem_etc:
        # 排除创业板(300, 301), 科创板(688), 北交所(8, 4)
        cond &= ~df["code"].str.startswith(("300", "301", "688", "8", "4"))

    codes = df.loc[cond, "code"].str.zfill(6).tolist()

    # 附加股票池 appendix.json
    try:
        with open("appendix.json", "r", encoding="utf-8") as f:
            appendix_codes = json.load(f)["data"]
    except FileNotFoundError:
        appendix_codes = []
    
    # 合并并去重，保持顺序
    final_codes = sorted(list(dict.fromkeys(appendix_codes + codes)))

    logger.info("根据市值筛选得到 %d 只股票，与附加文件合并后共 %d 只。", len(codes), len(final_codes))
    return final_codes


# --------------------------- 历史 K 线抓取 --------------------------- #

_FREQ_MAP = {
    0: "5m", 1: "15m", 2: "30m", 3: "1h", 4: "day", 5: "week", 6: "mon",
    7: "1m", 8: "1m", 9: "day", 10: "3mon", 11: "year",
}

# ---------- Mootdx 数据源 ---------- #

def _get_kline_mootdx(code: str, start: str, end: str, freq_code: int) -> pd.DataFrame:
    """使用Mootdx获取K线数据"""
    symbol = code.zfill(6)
    freq_str = _FREQ_MAP.get(freq_code, "day")
    client = Quotes.factory(market="std") # 'std' 默认使用前复权数据

    try:
        df = client.bars(symbol=symbol, frequency=freq_str, start_date=start, end_date=end)
    except Exception as e:
        logger.debug("Mootdx 拉取 %s 失败: %s", code, e)
        return pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame()

    df = df.rename(columns={"datetime": "date", "vol": "volume"})
    df["date"] = pd.to_datetime(df["date"])
    
    start_ts = pd.to_datetime(start, format="%Y%m%d")
    end_ts = pd.to_datetime(end, format="%Y%m%d")
    df = df[(df["date"].dt.date >= start_ts.date()) & (df["date"].dt.date <= end_ts.date())].copy()
    df = df.sort_values("date").reset_index(drop=True)

    return df[["date", "open", "close", "high", "low", "volume"]]

# ---------- 东方财富数据源 (备用) ---------- #

def _create_eastmoney_url(code: str, limit: int = 10000) -> str:
    """构建东方财富API URL"""
    secid = f"1.{code}" if code.startswith("6") else f"0.{code}"
    timestamp = int(round(time.time() * 1000))
    base_url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "secid": secid, "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101", "fqt": "1", "end": "20500101", "lmt": str(limit), "_": str(timestamp)
    }
    return f"{base_url}?{'&'.join([f'{k}={v}' for k, v in params.items()])}"

def _parse_eastmoney_data(response_text: str, code: str, start: str, end: str) -> pd.DataFrame:
    """解析东方财富API返回的数据"""
    try:
        data = json.loads(response_text)
        if not data.get('data') or not data['data'].get('klines'):
            return pd.DataFrame()

        rows = [{'date': r.split(',')[0], 'open': float(r.split(',')[1]), 'close': float(r.split(',')[2]),
                 'high': float(r.split(',')[3]), 'low': float(r.split(',')[4]), 'volume': float(r.split(',')[5])}
                for r in data['data']['klines'] if len(r.split(',')) >= 6]
        
        if not rows: return pd.DataFrame()
        df = pd.DataFrame(rows)
        df['date'] = pd.to_datetime(df['date'])

        start_ts = pd.to_datetime(start, format="%Y%m%d")
        end_ts = pd.to_datetime(end, format="%Y%m%d")
        df = df[(df['date'] >= start_ts) & (df['date'] <= end_ts)].copy()
        
        return df.sort_values('date').reset_index(drop=True)
    except Exception as e:
        logger.debug("解析东方财富数据失败 %s: %s", code, e)
        return pd.DataFrame()

def _get_kline_eastmoney(code: str, start: str, end: str) -> pd.DataFrame:
    """使用东方财富API获取K线数据"""
    url = _create_eastmoney_url(code)
    for attempt in range(1, 4):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            df = _parse_eastmoney_data(response.text, code, start, end)
            if not df.empty: return df
        except requests.RequestException as e:
            logger.debug("东方财富网络请求 %s 第 %d 次失败: %s", code, attempt, e)
            time.sleep(random.uniform(0.5, 1.5) * attempt)
    return pd.DataFrame()


# ---------- 数据校验与处理 ---------- #

def validate_and_clean(df: pd.DataFrame) -> pd.DataFrame:
    """数据校验和清理"""
    if df.empty: return df
    df = df.drop_duplicates(subset="date").sort_values("date").reset_index(drop=True)
    if df.isnull().values.any(): raise ValueError("数据中存在空值！")
    if (df["date"] > pd.Timestamp.now(tz='Asia/Shanghai')).any():
        raise ValueError("数据包含未来日期，可能抓取错误！")
    return df

# ---------- 单只股票抓取 (支持备用数据源) ---------- #

def fetch_one(
    code: str, start: str, end: str, out_dir: Path, incremental: bool,
    freq_code: int, use_backup: bool = True
):
    """抓取单只股票的K线数据，支持备用数据源"""
    csv_path = out_dir / f"{code}.csv"
    original_start = start

    if incremental and csv_path.exists():
        try:
            existing = pd.read_csv(csv_path, parse_dates=["date"])
            if not existing.empty:
                last_date = existing["date"].max()
                if last_date.date() >= pd.to_datetime(end, format="%Y%m%d").date():
                    return {"code": code, "status": "up_to_date", "source": "local"}
                start = (last_date + pd.Timedelta(days=1)).strftime("%Y%m%d")
        except Exception:
            logger.exception("读取 %s 失败，将重新全量下载", csv_path)

    # 尝试主数据源 Mootdx
    new_df = _get_kline_mootdx(code, start, end, freq_code)
    data_source = "mootdx" if not new_df.empty else "none"

    # 如果 Mootdx 失败且启用备用源 (仅日线支持)
    if new_df.empty and use_backup and freq_code == 4:
        logger.info("%s Mootdx拉取失败，尝试东方财富备用源", code)
        new_df = _get_kline_eastmoney(code, start, end)
        if not new_df.empty: data_source = "eastmoney"

    if new_df.empty:
        logger.warning("%s 所有数据源均无法获取数据", code)
        return {"code": code, "status": "failed", "source": "none"}

    try:
        new_df = validate_and_clean(new_df)
        if incremental and csv_path.exists():
            old_df = pd.read_csv(csv_path, parse_dates=["date"])
            combined_df = pd.concat([old_df, new_df], ignore_index=True) \
                            .drop_duplicates(subset="date", keep="last") \
                            .sort_values("date")
        else:
            combined_df = new_df

        combined_df.to_csv(csv_path, index=False)
        return {"code": code, "status": "success", "source": data_source}
    except Exception as e:
        logger.error("%s 数据处理或保存失败: %s", code, e)
        return {"code": code, "status": "failed", "source": data_source, "error": str(e)}


# --------------------------- 主入口 --------------------------- #

def main():
    parser = argparse.ArgumentParser(description="通过市值筛选A股，并使用Mootdx+东方财富备用源抓取历史K线")
    parser.add_argument("--frequency", type=int, choices=list(_FREQ_MAP.keys()), default=4,
                       help="K线频率编码. 默认: 4 (日线)")
    parser.add_argument("--exclude-gem", action="store_true", help="排除创业板/科创板/北交所股票")
    parser.add_argument("--min-mktcap", type=float, default=5e9,
                       help="最小总市值(含), 单位:元. 默认50亿")
    parser.add_argument("--max-mktcap", type=float, default=float("inf"),
                       help="最大总市值(含), 单位:元. 默认无限制")
    parser.add_argument("--start", default="20190101", help="起始日期 YYYYMMDD. 默认: 20190101")
    parser.add_argument("--end", default="today", help="结束日期 YYYYMMDD 或 'today'. 默认: today")
    parser.add_argument("--out", default="./data", help="输出目录. 默认: ./data")
    parser.add_argument("--workers", type=int, default=10, help="并发线程数. 默认: 10")
    parser.add_argument("--no-backup", action="store_true", help="禁用东方财富备用数据源")
    args = parser.parse_args()

    start_date = dt.date.today().strftime("%Y%m%d") if args.start.lower() == "today" else args.start
    end_date = dt.date.today().strftime("%Y%m%d") if args.end.lower() == "today" else args.end
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    use_backup = not args.no_backup and args.frequency == 4
    logger.info("数据源配置: Mootdx(主) | 东方财富(备用, 日线)")
    logger.info("K线频率: %s (%d)", _FREQ_MAP[args.frequency], args.frequency)

    # 市值快照 & 股票池
    mktcap_df = _get_mktcap_eastmoney()
    codes_from_filter = get_constituents(args.min_mktcap, args.max_mktcap, args.exclude_gem, mktcap_df)
    local_codes = [p.stem for p in out_dir.glob("*.csv")]
    codes = sorted(list(set(codes_from_filter) | set(local_codes)))

    if not codes:
        logger.error("最终股票池为空，请调整筛选参数或检查本地数据！")
        sys.exit(1)

    logger.info("开始抓取 %d 支股票 | 日期范围: %s -> %s", len(codes), start_date, end_date)

    # 多线程抓取
    results = {"success": 0, "failed": 0, "up_to_date": 0, "mootdx_count": 0, "eastmoney_count": 0, "failed_codes": []}
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(fetch_one, code, start_date, end_date, out_dir, True, args.frequency, use_backup): code for code in codes}
        for future in tqdm(as_completed(futures), total=len(codes), desc="下载进度"):
            res = future.result()
            if not res: continue
            status, source = res.get("status", "failed"), res.get("source", "none")
            results[status] += 1
            if source == "mootdx": results["mootdx_count"] += 1
            elif source == "eastmoney": results["eastmoney_count"] += 1
            if status == "failed": results["failed_codes"].append(res["code"])

    # 输出统计结果
    total_tasks = len(codes)
    logger.info("=" * 50)
    logger.info("抓取任务完成统计 (总任务数: %d)", total_tasks)
    logger.info("  - 成功: %d, 失败: %d, 已是最新: %d", results["success"], results["failed"], results["up_to_date"])
    logger.info("数据源使用统计 - Mootdx: %d, 东方财富: %d", results["mootdx_count"], results["eastmoney_count"])
    if results["failed_codes"]:
        logger.warning("失败股票代码: %s", ", ".join(results["failed_codes"]))
    if total_tasks > 0:
        success_rate = (results["success"] + results["up_to_date"]) / total_tasks * 100
        logger.info("数据下载完整率: %.1f%%", success_rate)
    logger.info("全部任务完成，数据已保存至 %s", out_dir.resolve())

if __name__ == "__main__":
    main()
