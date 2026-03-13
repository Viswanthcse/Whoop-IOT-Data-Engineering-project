from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base

# Simulating Cloud SQL with local SQLite for testing
DATABASE_URL = "sqlite:///./cloud_sql_sim.db"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
