__all__ = [
    "get_engine",
    "plan_route",
    "admin_event_rates",
    "road_segment_risk",
    "update_risk_table",
]
__version__ = "0.1.0"

try:  # optional dependencies may be missing during tests
    from .db import get_engine
except Exception:  # pragma: no cover
    get_engine = None

try:
    from .risk_tsp import plan_route
except Exception:  # pragma: no cover
    plan_route = None

try:
    from .bayesian import admin_event_rates, road_segment_risk, update_risk_table
except Exception:  # pragma: no cover
    admin_event_rates = road_segment_risk = update_risk_table = None
