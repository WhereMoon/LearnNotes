#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
涨停选股策略脚本（简洁版）

策略条件：
- 当日涨停板股票
- 最新价 < 30 元
- 总市值 < 200 亿元
- 近半年涨停次数 >= 3
- 剔除连续涨停 4 天及以上的股票

输出：符合条件的股票代码 + 名称
"""

import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings("ignore")


def get_limit_up_stocks(date: str = None) -> pd.DataFrame:
    """
    获取指定日期的涨停板股票池（东方财富数据）

    Parameters
    ----------
    date : str
        日期，格式为 YYYYMMDD；None 表示今天
    """
    if date is None:
        date = datetime.now().strftime("%Y%m%d")

    try:
        df = ak.stock_zt_pool_em(date=date)
        # 常见列名（可能随时间略有调整）：
        # '代码', '名称', '最新价', '总市值', '连续涨停天数', '涨停统计', ...
        return df
    except Exception as e:
        print(f"获取 {date} 涨停池数据失败: {e}")
        return pd.DataFrame()


def filter_stocks(df: pd.DataFrame) -> pd.DataFrame:
    """
    按策略条件过滤涨停股
    """
    if df.empty:
        return df

    # 复制一份，避免修改原数据
    data = df.copy()

    # 统一列名（不同版本 akshare 字段可能有轻微差异，这里做一下兼容）
    col_code = "代码"
    col_name = "名称"

    # 最新价
    price_cols = ["最新价", "现价", "收盘价"]
    col_price = next((c for c in price_cols if c in data.columns), None)

    # 总市值（单位：亿元，注意可能是元需要转换）
    mktcap_cols = ["总市值", "总市值(亿)", "总市值-亿"]
    col_mktcap = next((c for c in mktcap_cols if c in data.columns), None)

    # 连续涨停天数
    lianban_cols = ["连续涨停天数", "连板数", "连板次数"]
    col_lianban = next((c for c in lianban_cols if c in data.columns), None)

    # 近半年涨停次数（注意"涨停统计"可能是"总次数/半年次数"格式）
    times_cols = ["涨停统计", "半年涨停次数", "近半年涨停次数"]
    col_times = next((c for c in times_cols if c in data.columns), None)
    
    # 如果找到"涨停统计"列，需要解析"总次数/半年次数"格式
    if col_times == "涨停统计" and col_times in data.columns:
        # 解析"总次数/半年次数"格式，提取后半部分（半年次数）
        def extract_half_year_times(x):
            if pd.isna(x):
                return 0
            try:
                if isinstance(x, str) and '/' in x:
                    parts = x.split('/')
                    if len(parts) >= 2:
                        return int(parts[1])  # 取后半部分（半年次数）
                return int(float(x))
            except:
                return 0
        
        data['近半年涨停次数_解析'] = data[col_times].apply(extract_half_year_times)
        col_times = '近半年涨停次数_解析'

    missing = []
    if col_price is None:
        missing.append("最新价")
    if col_mktcap is None:
        missing.append("总市值")
    if col_lianban is None:
        missing.append("连续涨停天数")
    if col_times is None:
        missing.append("涨停统计(近半年涨停次数)")

    if missing:
        print("数据列缺失，无法完整按策略过滤，缺失列：", ", ".join(missing))
        # 只返回代码和名称
        return data[[col_code, col_name]] if all(c in data.columns for c in [col_code, col_name]) else data

    # 转换数值类型，出错的设为 NaN
    for c in [col_price, col_mktcap, col_lianban, col_times]:
        if c:
            data[c] = pd.to_numeric(data[c], errors="coerce")

    # 处理市值单位：如果最大值大于1000，说明是元，需要除以1e8转换为亿元
    if col_mktcap:
        max_mktcap = data[col_mktcap].max()
        if pd.notna(max_mktcap) and max_mktcap > 1000:
            data[col_mktcap] = data[col_mktcap] / 1e8  # 转换为亿元

    # 条件过滤
    cond_price = data[col_price] < 30 if col_price else pd.Series([True] * len(data), index=data.index)  # 单价 < 30
    cond_mktcap = data[col_mktcap] < 200 if col_mktcap else pd.Series([True] * len(data), index=data.index)  # 总市值 < 200 亿
    cond_times = data[col_times] >= 3 if col_times else pd.Series([True] * len(data), index=data.index)  # 半年内涨停次数 >= 3
    cond_lianban_lt4 = data[col_lianban] < 4 if col_lianban else pd.Series([True] * len(data), index=data.index)  # 连板 < 4
    cond_first_board = data[col_lianban] == 1 if col_lianban else pd.Series([True] * len(data), index=data.index)  # 仅首板

    filtered = data[cond_price & cond_mktcap & cond_times & cond_lianban_lt4 & cond_first_board].copy()

    # 只保留关键信息
    keep_cols = [col_code, col_name]
    if col_price:
        keep_cols.append(col_price)
    if col_mktcap:
        keep_cols.append(col_mktcap)
    if col_lianban:
        keep_cols.append(col_lianban)
    if col_times:
        keep_cols.append(col_times)
    
    filtered = filtered[keep_cols]

    # 重命名为更直观的中文列名
    rename_map = {
        col_code: "代码",
        col_name: "名称",
    }
    if col_price:
        rename_map[col_price] = "最新价"
    if col_mktcap:
        rename_map[col_mktcap] = "总市值(亿)"
    if col_lianban:
        rename_map[col_lianban] = "连板数"
    if col_times:
        rename_map[col_times] = "近半年涨停次数"
    
    filtered.rename(columns=rename_map, inplace=True)

    # 按近半年涨停次数降序、连板数降序排序
    filtered.sort_values(
        by=["近半年涨停次数", "连板数", "总市值(亿)"],
        ascending=[False, False, True],
        inplace=True,
    )

    filtered.reset_index(drop=True, inplace=True)
    return filtered


def main():
    """主函数：统计并输出符合条件的涨停股"""
    print("=" * 70)
    print("当日涨停选股策略（30元以下 & 200亿以下 & 半年涨停≥3 & 剔除4连板及以上）")
    print("=" * 70)

    # 默认今天，也支持手动输入日期
    today_str = datetime.now().strftime("%Y%m%d")
    print(f"\n默认分析日期：{today_str}")
    user_date = input("如需指定日期，请输入 YYYYMMDD（直接回车使用默认日期）：").strip()
    if user_date:
        date = user_date
    else:
        date = today_str

    print(f"\n正在获取 {date} 的涨停板股票池数据...")
    df_zt = get_limit_up_stocks(date)

    if df_zt.empty:
        print("未获取到涨停数据，可能是休市日或网络问题。")
        return

    print(f"\n{'='*70}")
    print(f"📊 当日全部涨停股票（共 {len(df_zt)} 只）")
    print(f"{'='*70}")
    
    # 显示全部涨停股票
    if "代码" in df_zt.columns and "名称" in df_zt.columns:
        # 准备显示的列
        all_display_cols = ["代码", "名称"]
        
        # 添加价格列（如果存在）
        price_cols = ["最新价", "现价", "收盘价"]
        for col in price_cols:
            if col in df_zt.columns:
                all_display_cols.append(col)
                break
        
        # 添加市值列（如果存在，并转换为亿元显示）
        mktcap_cols = ["总市值", "总市值(亿)", "总市值-亿"]
        col_mktcap = None
        for col in mktcap_cols:
            if col in df_zt.columns:
                col_mktcap = col
                all_display_cols.append(col)
                break
        
        # 添加连板数列（如果存在）
        lianban_cols = ["连续涨停天数", "连板数", "连板次数"]
        for col in lianban_cols:
            if col in df_zt.columns:
                all_display_cols.append(col)
                break
        
        # 添加涨停统计列（如果存在）
        times_cols = ["涨停统计", "半年涨停次数", "近半年涨停次数"]
        for col in times_cols:
            if col in df_zt.columns:
                all_display_cols.append(col)
                break
        
        # 显示全部涨停股票
        all_cols = [c for c in all_display_cols if c in df_zt.columns]
        display_df = df_zt[all_cols].copy()
        
        # 如果市值是元为单位，转换为亿元显示
        if col_mktcap and col_mktcap in display_df.columns:
            try:
                display_df[col_mktcap] = pd.to_numeric(display_df[col_mktcap], errors='coerce')
                # 如果最大值大于1000，说明是元，需要除以1e8转换为亿元
                if display_df[col_mktcap].max() > 1000:
                    display_df[f"{col_mktcap}(亿)"] = display_df[col_mktcap] / 1e8
                    display_df = display_df.drop(columns=[col_mktcap])
                    all_cols = [c if c != col_mktcap else f"{col_mktcap}(亿)" for c in all_cols]
            except:
                pass
        
        print(display_df.to_string(index=False))
    else:
        # 如果列名不匹配，显示所有列
        print("可用列名：", list(df_zt.columns))
        print(df_zt.head(50).to_string(index=False))
        if len(df_zt) > 50:
            print(f"\n... 还有 {len(df_zt) - 50} 只股票未显示")

    # 按策略过滤
    print(f"\n{'='*70}")
    print("🔍 按策略条件筛选后的股票")
    print(f"{'='*70}")
    print("筛选条件：")
    print("  ✅ 最新价 < 30 元")
    print("  ✅ 总市值 < 200 亿元")
    print("  ✅ 近半年涨停次数 >= 3")
    print("  ✅ 剔除连续涨停 4 天及以上的股票")
    print("  ✅ 仅首板（连板数 = 1）")
    print(f"{'='*70}\n")
    
    result = filter_stocks(df_zt)

    if result.empty:
        print("暂无符合条件的股票。")
        return

    # 显示筛选后的结果
    display_cols = ["代码", "名称", "最新价", "总市值(亿)", "连板数", "近半年涨停次数"]
    display_cols = [c for c in display_cols if c in result.columns]

    print(f"符合条件的股票数量：{len(result)} 只\n")
    print(result[display_cols].to_string(index=False))


if __name__ == "__main__":
    main()

