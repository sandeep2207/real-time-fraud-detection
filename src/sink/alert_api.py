"""
Alert API — FastAPI sink for fraud alerts from Flink.

Endpoints:
  POST /alerts          — ingest alert from Flink sink
  GET  /alerts          — list recent alerts (paginated)
  GET  /alerts/{id}     — get single alert
  GET  /stats           — aggregated fraud stats
"""

from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import create_engine, Column, String, Float, DateTime, JSON, func
from sqlalchemy.orm import DeclarativeBase, Session
from datetime import datetime, timezone
import os
import json

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://fraud_user:fraud_pass@localhost:5432/fraud_detection")

engine = create_engine(DATABASE_URL)
app = FastAPI(title="Fraud Alert API", version="1.0.0")


class Base(DeclarativeBase):
    pass


class AlertModel(Base):
    __tablename__ = "fraud_alerts"

    alert_id     = Column(String, primary_key=True)
    transaction_id = Column(String, index=True)
    card_id      = Column(String, index=True)
    amount       = Column(Float)
    signal_type  = Column(String)
    score        = Column(Float)
    window_start = Column(String)
    window_end   = Column(String)
    detected_at  = Column(String)
    details      = Column(JSON)


Base.metadata.create_all(engine)


@app.post("/alerts", status_code=201)
def ingest_alert(payload: dict):
    with Session(engine) as session:
        alert = AlertModel(**payload)
        session.merge(alert)   # idempotent upsert
        session.commit()
    return {"status": "ok", "alert_id": payload.get("alert_id")}


@app.get("/alerts")
def list_alerts(
    signal_type: str | None = None,
    card_id: str | None = None,
    limit: int = Query(default=50, le=500),
    offset: int = 0,
):
    with Session(engine) as session:
        q = session.query(AlertModel)
        if signal_type:
            q = q.filter(AlertModel.signal_type == signal_type)
        if card_id:
            q = q.filter(AlertModel.card_id == card_id)
        total = q.count()
        alerts = q.order_by(AlertModel.detected_at.desc()).offset(offset).limit(limit).all()
        return {
            "total": total,
            "alerts": [
                {c.name: getattr(a, c.name) for c in AlertModel.__table__.columns}
                for a in alerts
            ],
        }


@app.get("/alerts/{alert_id}")
def get_alert(alert_id: str):
    with Session(engine) as session:
        alert = session.get(AlertModel, alert_id)
        if not alert:
            raise HTTPException(status_code=404, detail="Alert not found")
        return {c.name: getattr(alert, c.name) for c in AlertModel.__table__.columns}


@app.get("/stats")
def get_stats():
    with Session(engine) as session:
        total = session.query(func.count(AlertModel.alert_id)).scalar()
        by_signal = (
            session.query(AlertModel.signal_type, func.count(AlertModel.alert_id))
            .group_by(AlertModel.signal_type)
            .all()
        )
        avg_score = session.query(func.avg(AlertModel.score)).scalar()
        return {
            "total_alerts": total,
            "by_signal_type": dict(by_signal),
            "avg_fraud_score": round(float(avg_score or 0), 3),
        }


@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}
