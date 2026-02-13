from sqlalchemy import Column, Integer, Float, String, DateTime
from database import Base


class SensorData(Base):
    __tablename__ = "sensor_data"

    id = Column(Integer, primary_key=True, index=True)
    device_id = Column(String)
    temperature = Column(Float)
    humidity = Column(Float)
    timestamp = Column(DateTime)
