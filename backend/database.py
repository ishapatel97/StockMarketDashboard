from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base

# ⚠️ TEMPORARY - Connected to LIVE Render database from local machine
# TODO: Revert this before pushing to GitHub!
DATABASE_URL = "postgresql://stockmarket_db_6z4q_user:App1I3cu54KCLdVlYKalVFZlIr4v9uvO@dpg-d6n3u8450q8c73atq74g-a.virginia-postgres.render.com/stockmarket_db_6z4q"
POLYGON_API_KEY="vEHMinQC5ikgbqR9krTrxxKyRaUwUU3L"
# -- PRODUCTION (restore this when done) --
# import os
# DATABASE_URL = os.getenv("DATABASE_URL")

# -- LOCAL --
# DATABASE_URL = "postgresql://postgres:1234@localhost:5433/stock_dashboard"


engine = create_engine(DATABASE_URL)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)
Base = declarative_base()