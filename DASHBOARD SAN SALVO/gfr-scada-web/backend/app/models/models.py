from sqlalchemy import Column, Integer, String, DateTime, Float, Boolean, ForeignKey, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.db import Base


class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    username = Column(String(50), unique=True, index=True, nullable=False)
    hashed_password = Column(String(255), nullable=False)
    role = Column(String(20), default='viewer')
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class Plant(Base):
    __tablename__ = 'plants'
    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class Room(Base):
    __tablename__ = 'rooms'
    id = Column(Integer, primary_key=True)
    plant_id = Column(Integer, ForeignKey('plants.id'), nullable=False)
    name = Column(String(100), nullable=False)
    plant = relationship('Plant')


class CommandRequest(Base):
    __tablename__ = 'commands'
    id = Column(Integer, primary_key=True)
    requested_by = Column(Integer, ForeignKey('users.id'))
    command = Column(String(50))
    target = Column(String(100))
    params = Column(Text)
    status = Column(String(30), default='requested')
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    approved_by = Column(Integer, ForeignKey('users.id'), nullable=True)
    executed_by = Column(Integer, ForeignKey('users.id'), nullable=True)


class AlarmEvent(Base):
    __tablename__ = 'alarms'
    id = Column(Integer, primary_key=True)
    plant = Column(String(100))
    room = Column(String(100))
    signal = Column(String(100))
    severity = Column(String(20))
    message = Column(Text)
    active = Column(Boolean, default=True)
    ack_user = Column(String(50), nullable=True)
    ack_time = Column(DateTime(timezone=True), nullable=True)
    ts = Column(DateTime(timezone=True), server_default=func.now())


class Measurement(Base):
    __tablename__ = 'measurements'

    id = Column(Integer, primary_key=True, index=True)
    plant = Column(String(100), index=True)
    room = Column(String(100), index=True)
    signal = Column(String(100), index=True)
    value = Column(Float)
    unit = Column(String(50))
    ts = Column(DateTime(timezone=True), index=True)
