from pathfinder.risk_tsp import haversine, distance_matrix, nearest_neighbor
from pathfinder.bayesian import estimate_event_rate
import pandas as pd


def test_haversine():
    d = haversine(0, 0, 0, 1)
    assert round(d, 2) == 111.19


def test_distance_matrix_and_nn():
    df = pd.DataFrame({
        'lon': [0, 0.1, 0.2],
        'lat': [0, 0.1, 0.2],
        'risk': [0, 0, 0],
    })
    mat = distance_matrix(df)
    order = nearest_neighbor(mat)
    assert order[0] == 0
    assert len(order) == 3


def test_estimate_event_rate():
    df = pd.DataFrame({
        'admin2': ['a', 'a', 'b'],
        'events': [2, 1, 0],
    })
    res = estimate_event_rate(df, alpha=1, beta=1)
    rate_a = res.loc[res.admin2=='a', 'pred_rate'].iloc[0]
    assert round(rate_a, 2) == 1.00
