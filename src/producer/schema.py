from pydantic import BaseModel, Field
from datetime import datetime


class Transaction(BaseModel):
    transaction_id: str
    card_id: str
    amount: float = Field(gt=0)
    currency: str = "INR"
    merchant: str
    country: str
    city: str
    timestamp: str
    is_fraud_label: bool = False  # ground truth label for benchmarking only


class FraudAlert(BaseModel):
    alert_id: str
    transaction_id: str
    card_id: str
    amount: float
    signal_type: str          # "velocity" | "amount_deviation" | "geography"
    score: float = Field(ge=0.0, le=1.0)
    window_start: str
    window_end: str
    detected_at: str
    details: dict = {}
