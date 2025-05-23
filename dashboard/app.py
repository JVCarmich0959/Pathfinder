import streamlit as st
import pandas as pd
import altair as alt
from pathfinder.db import get_engine

st.set_page_config(page_title="ACLED Last 12 Months", layout="wide")

MONTHS_MAX = 12

@st.cache_data
def load_data():
    sql = """
    SELECT month_start, admin1, admin2, events, fatalities
    FROM acled_monthly_clean
    WHERE month_start >= (date_trunc('month', CURRENT_DATE) - INTERVAL '11 months')
    """
    return pd.read_sql(sql, get_engine(), parse_dates=['month_start'])


@st.cache_data
def aggregate_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate events and fatalities per month."""
    return (
        df.groupby("month_start")[["events", "fatalities"]]
        .sum()
        .reset_index()
    )


@st.cache_data
def events_heatmap(df: pd.DataFrame) -> pd.DataFrame:
    """Compute heatmap matrix of events by admin2 and month."""
    heat = (
        df.groupby(["admin2", "month_start"])["events"]
        .sum()
        .unstack(fill_value=0)
    )
    return heat.reset_index().melt("admin2", var_name="month", value_name="events")


@st.cache_data
def top_admin2(df: pd.DataFrame, n: int) -> pd.DataFrame:
    """Return top N admin2 areas ranked by event count."""
    return (
        df.groupby("admin2")["events"]
        .sum()
        .sort_values(ascending=False)
        .head(n)
        .reset_index()
    )

df = load_data()
if df.empty:
    st.error("No data returned from database")
    st.stop()

# ── Sidebar filters ──────────────────────────────────────────────
st.sidebar.header("Filters")
months = st.sidebar.slider("Months to show", 3, MONTHS_MAX, MONTHS_MAX)
admin1_opts = ["All"] + sorted(df["admin1"].dropna().unique().tolist())
admin1 = st.sidebar.selectbox("Admin1 region", admin1_opts)

end_date = df["month_start"].max()
start_date = end_date - pd.DateOffset(months=months - 1)
filtered = df[df["month_start"] >= start_date]
if admin1 != "All":
    filtered = filtered[filtered["admin1"] == admin1]

# ── Key metrics ──────────────────────────────────────────────────
totals = filtered[["events", "fatalities"]].sum()
col1, col2 = st.columns(2)
col1.metric("Total events", int(totals["events"]))
col2.metric("Total fatalities", int(totals["fatalities"]))

# Monthly totals line chart
monthly = aggregate_monthly(filtered)
st.header("Monthly totals")
source = monthly.melt('month_start', var_name='metric', value_name='count')
line = alt.Chart(source).mark_line(point=True).encode(
    x='month_start:T',
    y='count:Q',
    color='metric:N',
    tooltip=['month_start','metric','count']
).properties(height=300)
st.altair_chart(line, use_container_width=True)

# Heatmap of events by admin2 per month
st.header("Events heatmap by admin2")
heat_data = events_heatmap(filtered)
chart = alt.Chart(heat_data).mark_rect().encode(
    x='month:T',
    y=alt.Y('admin2:N', sort='-x'),
    color=alt.Color('events:Q', scale=alt.Scale(scheme='reds')),
    tooltip=['admin2','month','events']
).properties(height=400)
st.altair_chart(chart, use_container_width=True)

# Top N risky admin2
N = st.slider('Top N admin2 areas by events', 5, 20, 10)
top_admin2_df = top_admin2(filtered, N)
st.header("Top risky admin2")
st.dataframe(top_admin2_df)

# Allow export of filtered data
csv = filtered.to_csv(index=False).encode("utf-8")
st.download_button(
    label="Download data as CSV",
    data=csv,
    file_name="acled_filtered.csv",
    mime="text/csv",
)
