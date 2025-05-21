import streamlit as st
import pandas as pd
from pathfinder.db import get_engine

st.set_page_config(page_title="ACLED Last 12 Months", layout="wide")

@st.cache_data
def load_data():
    sql = """
    SELECT month_start, admin1, admin2, events, fatalities
    FROM acled_monthly_clean
    WHERE month_start >= (date_trunc('month', CURRENT_DATE) - INTERVAL '11 months')
    """
    return pd.read_sql(sql, get_engine(), parse_dates=['month_start'])

df = load_data()
if df.empty:
    st.error("No data returned from database")
    st.stop()

# Monthly totals line chart
monthly = df.groupby('month_start')[['events','fatalities']].sum().reset_index()
st.header("Monthly totals")
st.line_chart(monthly.set_index('month_start'))

# Heatmap of events by admin2 per month
st.header("Events heatmap by admin2")
heat = (
    df.groupby(['admin2','month_start'])['events']
      .sum()
      .unstack(fill_value=0)
)
heat_data = heat.reset_index().melt('admin2', var_name='month', value_name='events')
import altair as alt
chart = alt.Chart(heat_data).mark_rect().encode(
    x='month:T',
    y=alt.Y('admin2:N', sort='-x'),
    color=alt.Color('events:Q', scale=alt.Scale(scheme='reds')),
    tooltip=['admin2','month','events']
).properties(height=400)
st.altair_chart(chart, use_container_width=True)

# Top N risky admin2
N = st.slider('Top N admin2 areas by events', 5, 20, 10)
top_admin2 = (
    df.groupby('admin2')['events']
      .sum()
      .sort_values(ascending=False)
      .head(N)
      .reset_index()
)
st.header("Top risky admin2")
st.dataframe(top_admin2)
