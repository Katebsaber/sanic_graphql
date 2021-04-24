from database import Base
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, func
from sqlalchemy.orm import backref, relationship


def create_candle_model(table_name):

    class CandleModel(Base):
        __tablename__ = table_name
        __table_args__ = {'extend_existing': True}
        open_time = Column(Integer, primary_key=True)
        close_time = Column(Integer)
        open_price = Column(String)
        close_price = Column(String)
        low_price = Column(String)
        high_price = Column(String)
        volume = Column(String)

    return CandleModel
