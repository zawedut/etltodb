import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()


TABLE_NAME = "sbi_transactions_test"
SKIP_PREFIXES = ("SBI", "Profit", "Date", "---", "Buy-Date", "Page")


def clean_money(val):
    """ลบ comma และแปลงวงเล็บเป็นค่าลบ เช่น '(600.00)' -> -600.0"""
    val = str(val).replace(",", "")
    if val.startswith("(") and val.endswith(")"):
        val = "-" + val[1:-1]
    return float(val)


def is_date(text):
    """ตรวจสอบรูปแบบวันที่ DD/MM/YYYY"""
    return len(text) == 10 and text[2] == "/" and text[5] == "/"


def parse_row(tokens, has_date):
    """แยก tokens เป็น dict ของแต่ละ transaction"""
    offset = 1 if has_date else 0
    return {
        "sharecode": tokens[offset],
        "unit": clean_money(tokens[offset + 1]),
        "avg_cost": clean_money(tokens[offset + 2]),
        "buy_amount": clean_money(tokens[offset + 3]),
        "sell_date": tokens[offset + 4],
        "sell_price": clean_money(tokens[offset + 5]),
        "sell_amount": clean_money(tokens[offset + 6]),
        "profit_loss": clean_money(tokens[offset + 7]),
        "pl_percent": clean_money(tokens[offset + 8]),
    }


def extract(file_path):
    """อ่านไฟล์ Statement แล้วแยกข้อมูลแต่ละ transaction ออกมาเป็น list of dict"""
    raw_data = []
    current_account = None
    current_buy_date = None

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            # ดึงเลข Account
            if line.startswith("Account :"):
                current_account = line.split()[2]
                continue

            # ข้ามบรรทัด header, เส้นขีด, และแถวรวมยอด
            if line.startswith(SKIP_PREFIXES) or "* Total *" in line:
                continue

            tokens = line.split()
            if len(tokens) < 9:
                continue

            # parse ข้อมูลแต่ละแถว
            has_date = is_date(tokens[0])
            if has_date:
                current_buy_date = tokens[0]

            row = parse_row(tokens, has_date)
            row["account"] = current_account
            row["buy_date"] = current_buy_date
            raw_data.append(row)

    return raw_data


def transform(raw_data):
    """แปลง raw data เป็น DataFrame พร้อมจัดรูปแบบวันที่"""
    df = pd.DataFrame(raw_data)
    df["buy_date"] = pd.to_datetime(df["buy_date"], format="%d/%m/%Y")
    df["sell_date"] = pd.to_datetime(df["sell_date"], format="%d/%m/%Y")
    return df


def load(df, db_url):
    """โหลด DataFrame เข้า PostgreSQL"""
    engine = create_engine(db_url)
    df.to_sql(
        name=TABLE_NAME,
        con=engine,
        schema="public",
        if_exists="append",
        index=False,
    )
    return len(df)


def run_etl_pnl(file_path, db_url):
    """รัน ETL pipeline: Extract -> Transform -> Load"""
    print(f"🔄 กำลังอ่านไฟล์: {file_path}")

    # Extract
    raw_data = extract(file_path)

    # Transform
    df = transform(raw_data)
    print(f"✅ Transform เสร็จ ({len(df)} รายการ)")
    print(df.head())

    # Load
    print(f"\n🚀 กำลังโหลดเข้าตาราง '{TABLE_NAME}'...")
    try:
        count = load(df, db_url)
        print(f"🎉 สำเร็จ! นำเข้า {count} รายการ")
    except Exception as e:
        print(f"❌ เกิดข้อผิดพลาด: {e}")


if __name__ == "__main__":
    FILE_PATH = os.getenv("FILE_PATH", "SBI_Mock_Statement_Account_1234567890.txt")
    DB_URL = os.getenv("DB_URL")

    if not DB_URL:
        print("❌ กรุณาตั้งค่า DB_URL ในไฟล์ .env")
        exit(1)

    run_etl_pnl(FILE_PATH, DB_URL)