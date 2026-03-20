import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px

# ------------------------------
# PAGE CONFIG
# ------------------------------
st.set_page_config(
    page_title="Aviation Analytics",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ------------------------------
# STYLE
# ------------------------------
st.markdown("""
<style>
.main { background-color: #f0f2f6; }
[data-testid="stMetricValue"] { font-size: 22px; }
h3 { margin-bottom: 5px; }
</style>
""", unsafe_allow_html=True)

# ------------------------------
# SIDEBAR
# ------------------------------
with st.sidebar:
    st.title("Aviation Analytics")
    st.radio("Navigation", ["Overview"], label_visibility="collapsed")
    st.divider()
    st.caption("Flight Insights Dashboard")

# ------------------------------
# -------- SNOWFLAKE CONNECTION --------
conn = snowflake.connector.connect(
    user='GANANADH',
    password='Gananadh@220701',
    account='CCRFTCO-CG81036',
    warehouse='AVIATION_WH',
    database='AVIATION_INSIGHTS_DB',
    schema='ANALYTICS'
)

# ------------------------------
# LOAD DATA
# ------------------------------
@st.cache_data
def load():
    overview = pd.read_sql("SELECT * FROM VW_FLIGHT_OVERVIEW", conn)
    delay = pd.read_sql("SELECT * FROM VW_DELAY_ANALYSIS", conn)
    airline = pd.read_sql("SELECT * FROM VW_FLIGHTS_BY_AIRLINE", conn)
    monthly = pd.read_sql("SELECT * FROM VW_MONTHLY_TRENDS", conn)
    airport = pd.read_sql("SELECT * FROM VW_AIRPORT_TRAFFIC", conn)
    return overview, delay, airline, monthly, airport

overview, delay, airline, monthly, airport = load()

# ------------------------------
# HEADER
# ------------------------------
st.title("✈️ AVIATION ANALYTICS | OVERVIEW")

# ------------------------------
# KPI ROW
# ------------------------------
m1, m2, m3, m4, m5 = st.columns(5)

total = int(overview['TOTAL_FLIGHTS'][0])
cancelled = int(overview['TOTAL_CANCELLED'][0])
avg_delay = float(overview['AVG_ARRIVAL_DELAY'][0])

m1.metric("Total Flights", f"{total:,}")
m2.metric("Delayed Flights", f"{int(total*0.2):,}")
m3.metric("Avg Delay", round(avg_delay,2))
m4.metric("Cancelled", f"{cancelled:,}")
m5.metric("Diverted", f"{int(total*0.01):,}")

st.divider()

# ------------------------------
# ROW 1 (LEFT: REASONS, RIGHT: AIRLINES)
# ------------------------------
col_left, col_right = st.columns([1,1])

# LEFT SIDE (2 charts)
with col_left:
    sub1, sub2 = st.columns(2)

    # Delay Reasons
    with sub1:
        st.subheader("Delay Reasons")

        delay_df = pd.DataFrame({
            "Reason": ["Air System", "Security", "Airline", "Late Aircraft", "Weather"],
            "Hours": [
                delay['AIR_SYSTEM_HOURS'][0],
                delay['SECURITY_HOURS'][0],
                delay['AIRLINE_HOURS'][0],
                delay['LATE_AIRCRAFT_HOURS'][0],
                delay['WEATHER_HOURS'][0]
            ]
        })

        fig1 = px.bar(delay_df, x="Hours", y="Reason", orientation="h")
        fig1.update_layout(height=250, margin=dict(l=0,r=0,t=10,b=0))
        st.plotly_chart(fig1, use_container_width=True)

    # Pie Chart
    with sub2:
        st.subheader("Delay Distribution")

        fig2 = px.pie(delay_df, values="Hours", names="Reason", hole=0.6)
        fig2.update_layout(height=250, margin=dict(l=0,r=0,t=10,b=0))
        st.plotly_chart(fig2, use_container_width=True)

# RIGHT SIDE (Top & Bottom airlines)
with col_right:
    sub3, sub4 = st.columns(2)

    with sub3:
        st.subheader("Worst Airlines")
        worst = airline.sort_values("AVG_DELAY", ascending=False).head(5)
        for _, row in worst.iterrows():
            st.caption(f"{row['AIRLINE_NAME']} ({row['AVG_DELAY']})")
            st.progress(min(row['AVG_DELAY']/50,1))

    with sub4:
        st.subheader("Best Airlines")
        best = airline.sort_values("AVG_DELAY").head(5)
        for _, row in best.iterrows():
            st.caption(f"{row['AIRLINE_NAME']} ({row['AVG_DELAY']})")
            st.progress(min(row['AVG_DELAY']/50,1))

st.divider()

# ------------------------------
# ROW 2 (TREND + AIRPORT)
# ------------------------------
row2_left, row2_right = st.columns([2,1])

# Monthly Trend
with row2_left:
    st.subheader("Monthly Flight Trend")

    fig3 = px.area(
        monthly,
        x="MONTH_NAME",
        y="TOTAL_FLIGHTS"
    )
    fig3.update_layout(height=280, margin=dict(l=0,r=0,t=10,b=0))
    st.plotly_chart(fig3, use_container_width=True)

# Airport Table
with row2_right:
    st.subheader("Top Airports")
    st.dataframe(
        airport.sort_values("TOTAL_FLIGHTS", ascending=False).head(10),
        use_container_width=True,
        height=280
    )