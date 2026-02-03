from __future__ import annotations
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

load_dotenv()

def build_db_url() -> str:
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME", "controltower")
    user = os.getenv("DB_USER", "controltower")
    pwd = os.getenv("DB_PASSWORD", "controltower")
    return f"postgresql+psycopg://{user}:{pwd}@{host}:{port}/{name}"

def get_engine() -> Engine:
    return create_engine(build_db_url(), future=True)
