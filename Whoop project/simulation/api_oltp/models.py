from sqlalchemy import Column, Integer, String, Float, DateTime, Date, ForeignKey
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    id = Column(String, primary_key=True)
    first_name = Column(String)
    last_name = Column(String)
    dob = Column(Date)
    gender = Column(String)
    height_cm = Column(Float)
    weight_kg = Column(Float)

class DailyMetric(Base):
    __tablename__ = 'daily_metrics'
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String, ForeignKey('users.id'))
    date = Column(Date)
    recovery_score = Column(Integer, nullable=True)
    strain_score = Column(Float, nullable=True)
    total_sleep_seconds = Column(Integer, nullable=True)
    deep_sleep_seconds = Column(Integer, nullable=True)
    rem_sleep_seconds = Column(Integer, nullable=True)
    light_sleep_seconds = Column(Integer, nullable=True)
    awake_seconds = Column(Integer, nullable=True)
    rhr = Column(Integer, nullable=True)
    hrv_avg = Column(Float, nullable=True)
    respiratory_rate = Column(Float, nullable=True)
    error_flag = Column(String, nullable=True)

class Workout(Base):
    __tablename__ = 'workouts'
    id = Column(String, primary_key=True)
    user_id = Column(String, ForeignKey('users.id'))
    activity_type = Column(String)
    start_time = Column(DateTime)
    end_time = Column(DateTime)
