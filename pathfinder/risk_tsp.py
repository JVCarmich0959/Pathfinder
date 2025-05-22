"""Risk-aware route optimisation utilities."""

import math
import pandas as pd
from sqlalchemy import text

try:
    from ortools.constraint_solver import pywrapcp, routing_enums_pb2
    _ORTOOLS_AVAILABLE = True
except Exception:  # pragma: no cover - optional dep
    _ORTOOLS_AVAILABLE = False

from .db import get_engine


def haversine(lon1, lat1, lon2, lat2):
    """Return distance in kilometres between two WGS84 points."""
    rad = math.radians
    dlon = rad(lon2 - lon1)
    dlat = rad(lat2 - lat1)
    a = math.sin(dlat/2) ** 2 + math.cos(rad(lat1)) * math.cos(rad(lat2)) * math.sin(dlon/2) ** 2
    c = 2 * math.asin(math.sqrt(a))
    return 6371.0 * c


def fetch_road_risk(limit=50, engine=None):
    """Return road segments with event counts and midpoints."""
    if engine is None:
        engine = get_engine()
    sql = text(
        """
        SELECT r.id AS road_id,
               COALESCE(COUNT(e.event_id), 0) AS events,
               ST_X(ST_LineInterpolatePoint(r.geom, 0.5)) AS lon,
               ST_Y(ST_LineInterpolatePoint(r.geom, 0.5)) AS lat,
               ST_Length(r.geom::geography) AS length_m
        FROM sudan_roads_osm r
        LEFT JOIN events_near_primary_roads e ON e.road_id = r.id
        WHERE r.highway = 'primary'
        GROUP BY r.id, r.geom
        ORDER BY r.id
        LIMIT :lim
        """
    )
    df = pd.read_sql(sql, engine, params={"lim": limit})
    df["risk"] = df["events"] / df["length_m"].replace({0: 1})
    return df


def distance_matrix(df, alpha=1.0):
    """Weighted distance matrix for road midpoints."""
    n = len(df)
    mat = [[0.0] * n for _ in range(n)]
    for i in range(n):
        for j in range(i + 1, n):
            d = haversine(df.lon[i], df.lat[i], df.lon[j], df.lat[j])
            d *= 1 + alpha * (df.risk[i] + df.risk[j]) / 2
            mat[i][j] = mat[j][i] = d
    return mat


def nearest_neighbor(mat, start=0):
    """Very simple greedy TSP heuristic."""
    n = len(mat)
    visited = [False] * n
    order = [start]
    visited[start] = True
    for _ in range(n - 1):
        last = order[-1]
        choices = [(j, mat[last][j]) for j in range(n) if not visited[j]]
        if not choices:
            break
        nxt = min(choices, key=lambda x: x[1])[0]
        order.append(nxt)
        visited[nxt] = True
    return order


def ortools_tsp(mat):
    """Solve TSP using Google OR-Tools if available."""
    if not _ORTOOLS_AVAILABLE:
        return nearest_neighbor(mat)

    n = len(mat)
    routing = pywrapcp.RoutingModel(n, 1, 0)
    search = pywrapcp.DefaultRoutingSearchParameters()
    search.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC

    def cb(i, j):
        return int(mat[i][j] * 1000)

    routing.SetArcCostEvaluatorOfAllVehicles(cb)
    assignment = routing.SolveWithParameters(search)
    if assignment:
        index = routing.Start(0)
        order = []
        while not routing.IsEnd(index):
            order.append(routing.IndexToNode(index))
            index = assignment.Value(routing.NextVar(index))
        return order
    return nearest_neighbor(mat)


def plan_route(limit=50, alpha=1.0, engine=None, method="auto"):
    """Return an ordered dataframe of road segments for a route."""
    df = fetch_road_risk(limit=limit, engine=engine)
    mat = distance_matrix(df, alpha=alpha)
    if method == "ortools" or (method == "auto" and _ORTOOLS_AVAILABLE):
        order = ortools_tsp(mat)
    else:
        order = nearest_neighbor(mat)
    return df.iloc[order].assign(order=range(len(order)))
