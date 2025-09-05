from pathlib import Path
import polars as pl

def extract_rows_with_value(directory: str, column: str, value: str, delimiter: str = ",", output_file: str = "output.csv") -> int:
    """
    单次遍历目录及子目录下所有 CSV/TSV/TXT 文件，
    筛选目标列包含指定值的行，输出整行到 output_file，并统计总数。
    ZIP 文件会被跳过。
    """
    path = Path(directory)
    files = sorted(path.rglob("*"))

    if not files:
        print("未找到文件")
        return 0

    total_count = 0
    matched_rows = []

    for file in files:
        if file.is_file():
            # 跳过 ZIP 文件
            if str(file).lower().endswith(".zip"):
                continue

            try:
                # 读取整行 CSV
                df = pl.read_csv(
                    file,
                    separator=delimiter,
                    skip_rows=1,
                    ignore_errors=True,
                    truncate_ragged_lines=True
                )

                if df.height == 0 or column not in df.columns:
                    continue

                # 筛选匹配行
                matched_df = df.filter(pl.col(column).cast(pl.Utf8).str.contains(value))

                if matched_df.height > 0:
                    # 强制将所有列都转换为字符串
                    matched_df = matched_df.select([pl.col(c).cast(pl.Utf8) for c in matched_df.columns])
                    matched_rows.append(matched_df)
                    total_count += matched_df.height

            except Exception as e:
                print(f"跳过文件 {file}，无法读取为 CSV: {e}")

    # 合并所有匹配行并输出
    if matched_rows:
        # 使用 how="diagonal" 自动对齐不同列名
        result_df = pl.concat(matched_rows, how="diagonal")
        result_df.write_csv(output_file)
        print(f"匹配的整行已写入 {output_file}")

    return total_count


# 使用示例
if __name__ == "__main__":
    directory = r"D:\FModel\Output\Exports\Game\Content\Settings\dialogue"
    column = "Speaker"
    value = "猫汐尔"
    output_file = r".\matched_rows.csv"

    total_count = extract_rows_with_value(directory, column, value, delimiter="\t", output_file=output_file)
    print(f"值中包含 '{value}' 的记录总共出现 {total_count} 次")
