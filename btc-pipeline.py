from dataclasses import dataclass, asdict
from typing import Dict, Any, List
import json, os
from datetime import datetime

@dataclass
class Transaction:
    user_id: str
    btc_amount: float
    base_currency: str

class Filter:
    def process(self, context: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError

# -----------------------------
# Filtro 1: Validación
# -----------------------------
class ValidationFilter(Filter):
    SUPPORTED = {"USD", "EUR", "GBP"}
    def process(self, context: Dict[str, Any]) -> Dict[str, Any]:
        tx: Transaction = context.get("transaction")
        if tx is None:
            raise ValueError("Missing transaction in context")
        if not tx.user_id or not isinstance(tx.user_id, str):
            raise ValueError("Invalid user_id")
        if tx.btc_amount is None or tx.btc_amount <= 0:
            raise ValueError("btc_amount must be > 0")
        tx.base_currency = tx.base_currency.upper().strip()
        if tx.base_currency not in self.SUPPORTED:
            raise ValueError(f"Unsupported base currency: {tx.base_currency}")
        context["validated_at"] = datetime.utcnow().isoformat()
        print("[Validation] OK →", asdict(tx))
        return context

# -----------------------------
# Filtro 2: Autenticación (mock)
# -----------------------------
class AuthFilter(Filter):
    def __init__(self, users_db: Dict[str, Dict[str, Any]]):
        self.users_db = users_db
    def process(self, context: Dict[str, Any]) -> Dict[str, Any]:
        tx: Transaction = context["transaction"]
        user = self.users_db.get(tx.user_id)
        if not user:
            raise PermissionError("User not authenticated")
        context["user"] = user
        context["authenticated_at"] = datetime.utcnow().isoformat()
        print(f"[Auth] User '{tx.user_id}' authenticated")
        return context

# -----------------------------
# Filtro 3: Transformación (BTC→fiat)
# Servicio de tasas simulado, determinista y testeable
# -----------------------------
class FxService:
    def __init__(self, btc_usd: float = 68000.0, usd_to_eur: float = 0.92, usd_to_gbp: float = 0.78):
        self.btc_usd = btc_usd
        self.usd_to_eur = usd_to_eur
        self.usd_to_gbp = usd_to_gbp
    def btc_to_currency(self, btc_amount: float, currency: str) -> Dict[str, float]:
        usd_value = btc_amount * self.btc_usd
        if currency == "USD":
            return {"amount": usd_value, "fx": 1.0}
        elif currency == "EUR":
            return {"amount": usd_value * self.usd_to_eur, "fx": self.usd_to_eur}
        elif currency == "GBP":
            return {"amount": usd_value * self.usd_to_gbp, "fx": self.usd_to_gbp}
        else:
            raise ValueError(f"Unsupported currency: {currency}")


class TransformationFilter(Filter):
    def __init__(self, fx_service: FxService):
        self.fx = fx_service
    def process(self, context: Dict[str, Any]) -> Dict[str, Any]:
        tx: Transaction = context["transaction"]
        r = self.fx.btc_to_currency(tx.btc_amount, tx.base_currency)
        context["fiat_amount"] = round(r["amount"], 2)
        context["fx_used"] = {
            "btc_usd": self.fx.btc_usd,
            "usd_to_eur": self.fx.usd_to_eur,
            "usd_to_gbp": self.fx.usd_to_gbp,
            "applied": r["fx"],
        }
        context["transformed_at"] = datetime.utcnow().isoformat()
        print(f"[Transformation] {tx.btc_amount} BTC → {tx.base_currency} {context['fiat_amount']}")
        return context

# -----------------------------
# Filtro 4: Cálculo de Comisiones
# Comisión fija equivalente a 5.00 USD convertida a la moneda base
# -----------------------------
class FeeFilter(Filter):
    FIXED_FEE_USD = 5.00
    def __init__(self, fx_service: FxService):
        self.fx = fx_service
    def process(self, context: Dict[str, Any]) -> Dict[str, Any]:
        tx: Transaction = context["transaction"]
        if tx.base_currency == "USD":
            fee = self.FIXED_FEE_USD
        elif tx.base_currency == "EUR":
            fee = self.FIXED_FEE_USD * self.fx.usd_to_eur
        elif tx.base_currency == "GBP":
            fee = self.FIXED_FEE_USD * self.fx.usd_to_gbp
        else:
            raise ValueError("Unsupported currency for fee")
        fee = round(fee, 2)
        context["fee"] = fee
        context["total"] = round(context["fiat_amount"] + fee, 2)
        context["fee_calculated_at"] = datetime.utcnow().isoformat()
        print(f"[Fee] {tx.base_currency} {fee} | Total {tx.base_currency} {context['total']}")
        return context

# -----------------------------
# Filtro 5: Almacenamiento (JSON)
# -----------------------------
class StorageFilter(Filter):
    def __init__(self, storage_path: str = "transactions.json"):
        self.path = storage_path
    def process(self, context: Dict[str, Any]) -> Dict[str, Any]:
        record = {
            "timestamp": datetime.utcnow().isoformat(),
            "transaction": asdict(context["transaction"]),
            "user": context.get("user"),
            "fiat_amount": context.get("fiat_amount"),
            "fee": context.get("fee"),
            "total": context.get("total"),
            "fx_used": context.get("fx_used"),
        }
        existing: List[Dict[str, Any]] = []
        if os.path.exists(self.path):
            with open(self.path, "r", encoding="utf-8") as f:
                try:
                    existing = json.load(f)
                except json.JSONDecodeError:
                    existing = []
        existing.append(record)
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(existing, f, ensure_ascii=False, indent=2)
        context["persisted"] = True
        context["storage_path"] = os.path.abspath(self.path)
        print(f"[Storage] Saved in {self.path}")
        return context

# -----------------------------
# Orquestación de pipeline
# -----------------------------
class Pipeline:
    def __init__(self, filters: List[Filter]):
        self.filters = filters
    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        for f in self.filters:
            context = f.process(context)
        context["status"] = "SUCCESS"
        return context

# -----------------------------
# Ejecución de ejemplo
# -----------------------------
if __name__ == "__main__":
    USERS = {"u123": {"name": "Alice", "kyc_level": "basic"},
             "u456": {"name": "Bob", "kyc_level": "plus"}}
    fx = FxService(btc_usd=68000.0, usd_to_eur=0.92, usd_to_gbp=0.78)
    pipeline = Pipeline([
        ValidationFilter(),
        AuthFilter(USERS),
        TransformationFilter(fx),
        FeeFilter(fx),
        StorageFilter("transactions.json"),
    ])
    examples = [
        Transaction(user_id="u123", btc_amount=0.01, base_currency="EUR"),
        Transaction(user_id="u456", btc_amount=0.015, base_currency="USD"),
        Transaction(user_id="u123", btc_amount=0.02, base_currency="GBP"),
    ]
    for tx in examples:
        res = pipeline.run({"transaction": tx})
        printable = {k: (asdict(v) if k=="transaction" else v)
                     for k, v in res.items() if k != "user"}
        print("[Result]", json.dumps(printable, indent=2, ensure_ascii=False))
