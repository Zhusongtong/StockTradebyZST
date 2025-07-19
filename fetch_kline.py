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

import akshare as ak
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
logger = logging.getLogger("fetch_mktcap")

# 屏蔽第三方库多余 INFO 日志
for noisy in ("httpx", "urllib3", "_client", "akshare"):
    logging.getLogger(noisy).setLevel(logging.WARNING)

# --------------------------- 市值快照 --------------------------- #

def _get_mktcap_ak() -> pd.DataFrame:
    """实时快照，返回列：code, mktcap（单位：元）"""
    for attempt in range(1, 4):
        try:
            df = ak.stock_zh_a_spot_em()
            break
        except Exception as e:
            logger.warning("AKShare 获取市值快照失败(%d/3): %s", attempt, e)
            time.sleep(backoff := random.uniform(1, 3) * attempt)
    else:
        raise RuntimeError("AKShare 连续三次拉取市值快照失败！")

    df = df[["代码", "总市值"]].rename(columns={"代码": "code", "总市值": "mktcap"})
    df["mktcap"] = pd.to_numeric(df["mktcap"], errors="coerce")
    return df

# --------------------------- 股票池筛选 --------------------------- #

def get_constituents(
    min_cap: float,
    max_cap: float,
    small_player: bool,
    mktcap_df: Optional[pd.DataFrame] = None,
) -> List[str]:
    df = mktcap_df if mktcap_df is not None else _get_mktcap_ak()

    cond = (df["mktcap"] >= min_cap) & (df["mktcap"] <= max_cap)
    if small_player:
        cond &= ~df["code"].str.startswith(("300", "301", "688", "8", "4"))

    codes = df.loc[cond, "code"].str.zfill(6).tolist()

    # 附加股票池 appendix.json
    try:
        with open("appendix.json", "r", encoding="utf-8") as f:
            appendix_codes = json.load(f)["data"]
    except FileNotFoundError:
        appendix_codes = []
    codes = list(dict.fromkeys(appendix_codes + codes))  # 去重保持顺序

    logger.info("筛选得到 %d 只股票", len(codes))
    return codes

# --------------------------- 历史 K 线抓取 --------------------------- #

_FREQ_MAP = {
    0: "5m",
    1: "15m",
    2: "30m",
    3: "1h",
    4: "day",
    5: "week",
    6: "mon",
    7: "1m",
    8: "1m",
    9: "day",
    10: "3mon",
    11: "year",
}

# ---------- Mootdx 数据源 ---------- #

def _get_kline_mootdx(code: str, start: str, end: str, adjust: str, freq_code: int) -> pd.DataFrame:    
    """使用Mootdx获取K线数据"""
    symbol = code.zfill(6)
    freq = _FREQ_MAP.get(freq_code, "day")
    client = Quotes.factory(market="std")
    
    try:
        df = client.bars(symbol=symbol, frequency=freq, adjust=adjust or None)
    except Exception as e:
        logger.debug("Mootdx 拉取 %s 失败: %s", code, e)
        return pd.DataFrame()
        
    if df is None or df.empty:
        return pd.DataFrame()
    
    # 标准化列名
    df = df.rename(
        columns={
            "datetime": "date", 
            "open": "open", 
            "high": "high", 
            "low": "low", 
            "close": "close", 
            "vol": "volume"
        }
    )
    
    # 处理日期和数据筛选
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    start_ts = pd.to_datetime(start, format="%Y%m%d")
    end_ts = pd.to_datetime(end, format="%Y%m%d")
    df = df[(df["date"].dt.date >= start_ts.date()) & (df["date"].dt.date <= end_ts.date())].copy()    
    df = df.sort_values("date").reset_index(drop=True)    
    
    return df[["date", "open", "close", "high", "low", "volume"]]

# ---------- 东方财富数据源 (备用) ---------- #

def _create_eastmoney_url(code: str, limit: int = 10000) -> str:
    """构建东方财富API URL"""
    if code.startswith("6"):
        secid = f"1.{code}"
    elif code.startswith(("0", "3")):
        secid = f"0.{code}"
    else:
        secid = f"0.{code}"
    
    timestamp = int(round(time.time() * 1000))
    
    base_url = "http://55.push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "secid": secid,
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101",  # 日K线
        "fqt": "1",    # 前复权
        "end": "20500101",
        "lmt": str(limit),
        "_": str(timestamp)
    }
    
    url = base_url + "?" + "&".join([f"{k}={v}" for k, v in params.items()])
    return url

def _parse_eastmoney_data(response_text: str, code: str, start: str, end: str) -> pd.DataFrame:
    """解析东方财富API返回的数据"""
    try:
        data = json.loads(response_text)
        if not data.get('data') or not data['data'].get('klines'):
            return pd.DataFrame()
        
        klines = data['data']['klines']
        
        # 解析K线数据
        rows = []
        for line in klines:
            parts = line.split(',')
            if len(parts) >= 6:
                rows.append({
                    'date': parts[0],
                    'open': float(parts[1]),
                    'close': float(parts[2]),
                    'high': float(parts[3]),
                    'low': float(parts[4]),
                    'volume': float(parts[5])
                })
        
        if not rows:
            return pd.DataFrame()
        
        df = pd.DataFrame(rows)
        df['date'] = pd.to_datetime(df['date'])
        
        # 按日期范围筛选
        start_ts = pd.to_datetime(start, format="%Y%m%d")
        end_ts = pd.to_datetime(end, format="%Y%m%d")
        df = df[(df['date'].dt.date >= start_ts.date()) & 
                (df['date'].dt.date <= end_ts.date())].copy()
        
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
            if not df.empty:
                return df
        except Exception as e:
            logger.debug("东方财富拉取 %s 第 %d 次失败: %s", code, attempt, e)
            time.sleep(random.uniform(0.5, 1.5) * attempt)
    
    return pd.DataFrame()

# ---------- 数据校验 ---------- #

def validate(df: pd.DataFrame) -> pd.DataFrame:
    """数据校验和清理"""
    if df.empty:
        return df
        
    df = df.drop_duplicates(subset="date").sort_values("date").reset_index(drop=True)
    if df["date"].isna().any():
        raise ValueError("存在缺失日期！")
    if (df["date"] > pd.Timestamp.today()).any():
        raise ValueError("数据包含未来日期，可能抓取错误！")
    return df

def drop_dup_columns(df: pd.DataFrame) -> pd.DataFrame:
    """删除重复列"""
    return df.loc[:, ~df.columns.duplicated()]

# ---------- 单只股票抓取 (支持备用数据源) ---------- #

def fetch_one(
    code: str,
    start: str,
    end: str,
    out_dir: Path,
    incremental: bool,
    freq_code: int,
    use_backup: bool = True,
):
    """抓取单只股票的K线数据，支持备用数据源"""
    csv_path = out_dir / f"{code}.csv"

    # 增量更新：若本地已有数据则从最后一天开始
    original_start = start
    if incremental and csv_path.exists():
        try:
            existing = pd.read_csv(csv_path, parse_dates=["date"])
            last_date = existing["date"].max()
            if last_date.date() > pd.to_datetime(end, format="%Y%m%d").date():
                logger.debug("%s 已是最新，无需更新", code)
                return {"code": code, "status": "up_to_date", "source": "local"}
            start = last_date.strftime("%Y%m%d")
        except Exception:
            logger.exception("读取 %s 失败，将重新下载", csv_path)
            start = original_start

    # 首先尝试使用 Mootdx
    new_df = pd.DataFrame()
    data_source = "none"
    
    for attempt in range(1, 4):
        try:            
            new_df = _get_kline_mootdx(code, start, end, "qfq", freq_code)
            if not new_df.empty:
                data_source = "mootdx"
                break
        except Exception:
            logger.debug("%s Mootdx 第 %d 次抓取失败", code, attempt)
            time.sleep(random.uniform(0.5, 1.5) * attempt)
    
    # 如果 Mootdx 失败且启用备用数据源，尝试东方财富
    if new_df.empty and use_backup and freq_code == 4:  # 仅日线支持备用源
        logger.info("%s Mootdx拉取失败，尝试东方财富备用数据源", code)
        new_df = _get_kline_eastmoney(code, start, end)
        if not new_df.empty:
            data_source = "eastmoney"
    
    # 处理数据
    if new_df.empty:
        logger.warning("%s 所有数据源均无法获取数据", code)
        return {"code": code, "status": "failed", "source": "none"}
    
    try:
        new_df = validate(new_df)
        
        # 合并新旧数据
        if csv_path.exists() and incremental:
            old_df = pd.read_csv(csv_path, parse_dates=["date"], index_col=False)
            old_df = drop_dup_columns(old_df)
            new_df = drop_dup_columns(new_df)
            new_df = (
                pd.concat([old_df, new_df], ignore_index=True)
                .drop_duplicates(subset="date")
                .sort_values("date")
            )
        
        new_df.to_csv(csv_path, index=False)
        logger.debug("%s 数据更新完成 (来源: %s)", code, data_source)
        return {"code": code, "status": "success", "source": data_source}
        
    except Exception as e:
        logger.error("%s 数据处理失败: %s", code, e)
        return {"code": code, "status": "failed", "source": data_source, "error": str(e)}

# ---------- 主入口 ---------- #

def main():
    parser = argparse.ArgumentParser(description="使用Mootdx+东方财富备用源按市值筛选A股并抓取历史K线")
    parser.add_argument("--frequency", type=int, choices=list(_FREQ_MAP.keys()), default=4, 
                       help="K线频率编码: 0=5m, 1=15m, 2=30m, 3=1h, 4=day, 5=week, 6=mon, 7=1m, 8=1m, 9=day, 10=3mon, 11=year")
    parser.add_argument("--exclude-gem", action="store_true", default=True, 
                       help="排除创业板/科创板/北交所")
    parser.add_argument("--min-mktcap", type=float, default=5e9, 
                       help="最小总市值（含），单位：元，默认50亿")
    parser.add_argument("--max-mktcap", type=float, default=float("+inf"), 
                       help="最大总市值（含），单位：元，默认无限制")
    parser.add_argument("--start", default="20190101", 
                       help="起始日期 YYYYMMDD 或 'today'")
    parser.add_argument("--end", default="today", 
                       help="结束日期 YYYYMMDD 或 'today'")
    parser.add_argument("--out", default="./data", 
                       help="输出目录")
    parser.add_argument("--workers", type=int, default=10, 
                       help="并发线程数")
    parser.add_argument("--no-backup", action="store_true", 
                       help="禁用备用数据源，仅使用Mootdx")
    args = parser.parse_args()

    # ---------- 日期解析 ---------- #
    start = dt.date.today().strftime("%Y%m%d") if args.start.lower() == "today" else args.start
    end = dt.date.today().strftime("%Y%m%d") if args.end.lower() == "today" else args.end

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    use_backup = not args.no_backup and args.frequency == 4  # 仅日线支持备用源
    
    logger.info("数据源配置: Mootdx(主) + %s", 
                "东方财富(备用)" if use_backup else "无备用源")
    logger.info("K线频率: %s", _FREQ_MAP[args.frequency])

    # ---------- 市值快照 & 股票池 ---------- #
    logger.info("获取市值快照数据...")
    mktcap_df = _get_mktcap_ak()    

    codes_from_filter = get_constituents(
        args.min_mktcap,
        args.max_mktcap,
        args.exclude_gem,
        mktcap_df=mktcap_df,
    )    
    
    # 加上本地已有的股票，确保旧数据也能更新
    local_codes = [p.stem for p in out_dir.glob("*.csv")]
    codes = sorted(set(codes_from_filter) | set(local_codes))

    if not codes:
        logger.error("筛选结果为空，请调整参数！")
        sys.exit(1)

    logger.info(
        "开始抓取 %d 支股票 | 主源:mootdx | 备用源:%s | 频率:%s | 日期:%s → %s",
        len(codes),
        "东方财富" if use_backup else "无",
        _FREQ_MAP[args.frequency],
        start,
        end,
    )

    # ---------- 统计变量 ---------- #
    results = {
        "success": 0,
        "failed": 0,
        "up_to_date": 0,
        "mootdx_count": 0,
        "eastmoney_count": 0,
        "failed_codes": []
    }

    # ---------- 多线程抓取 ---------- #
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(
                fetch_one,
                code,
                start,
                end,
                out_dir,
                True,  # incremental
                args.frequency,
                use_backup,
            )
            for code in codes
        ]
        
        # 进度条显示和统计收集
        for future in tqdm(as_completed(futures), total=len(futures), desc="下载进度"):
            result = future.result()
            status = result["status"]
            source = result["source"]
            
            results[status] += 1
            
            if source == "mootdx":
                results["mootdx_count"] += 1
            elif source == "eastmoney":
                results["eastmoney_count"] += 1
            
            if status == "failed":
                results["failed_codes"].append(result["code"])

    # ---------- 输出统计结果 ---------- #
    logger.info("=" * 50)
    logger.info("抓取完成统计:")
    logger.info("成功: %d, 失败: %d, 已最新: %d", 
                results["success"], results["failed"], results["up_to_date"])
    logger.info("数据源统计 - Mootdx: %d, 东方财富: %d", 
                results["mootdx_count"], results["eastmoney_count"])
    
    if results["failed_codes"]:
        logger.warning("失败股票代码: %s", ", ".join(results["failed_codes"][:10]))
        if len(results["failed_codes"]) > 10:
            logger.warning("... 还有 %d 只股票失败", len(results["failed_codes"]) - 10)
    
    success_rate = (results["success"] + results["up_to_date"]) / len(codes) * 100
    logger.info("数据完整性: %.1f%% (%d/%d)", success_rate, 
                results["success"] + results["up_to_date"], len(codes))
    
    logger.info("全部任务完成，数据已保存至 %s", out_dir.resolve())

if __name__ == "__main__":
    main()
