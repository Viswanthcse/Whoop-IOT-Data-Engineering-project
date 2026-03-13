import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base

# For the simulation, we'll use SQLite locally, but this can be easily swapped 
# to a Cloud SQL PostgreSQL connection string via environment variables.

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./fitness_app_oltp.db")

engine = create_engine(
    DATABASE_URL, connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {}
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
