from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os

DATABASE_URL = os.getenv("DATABASE_URL")
#"postgresql://stockmarket_db_6z4q_user:App1I3cu54KCLdVlYKalVFZlIr4v9uvO@dpg-d6n3u8450q8c73atq74g-a/stockmarket_db_6z4q"
#"postgresql://postgres:1234@localhost:5433/stock_dashboard"


engine = create_engine(DATABASE_URL)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)