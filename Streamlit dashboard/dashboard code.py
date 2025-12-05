import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Aviation Dashboard", layout="wide")

st.title("✈️ Aviation Performance Dashboard")
st.caption("Interactive analytics for delays, cancellations & operational performance.")

session = get_active_session()

df = session.table("aviation_project.airlines.FACT_FLIGHTS").to_pandas()

# Build FLIGHT_DATE from DIM_DATE if missing
if "FLIGHT_DATE" not in df.columns or df["FLIGHT_DATE"].isna().all():
    dim_date = session.table("aviation_project.airlines.DIM_DATE").to_pandas()
    dim_date["FLIGHT_DATE"] = pd.to_datetime(
        dim_date[['YEAR','MONTH','DAY']].astype(str).agg('-'.join, axis=1)
    )
    df = df.merge(dim_date[['DATE_SK','FLIGHT_DATE']], on='DATE_SK', how='left')

df['FLIGHT_DATE'] = pd.to_datetime(df['FLIGHT_DATE'])

# Sidebar filters
st.sidebar.header("Filters")

min_date = df['FLIGHT_DATE'].min()
max_date = df['FLIGHT_DATE'].max()

date_range = st.sidebar.date_input(
    "Select Date Range",
    value=[min_date, max_date],
    min_value=min_date,
    max_value=max_date
)

# Airline filter
airlines = session.table("aviation_project.airlines.DIM_AIRLINES") \
                  .select("AIRLINE_NAME","IATA_CODE") \
                  .to_pandas()

airline_map = dict(zip(airlines["AIRLINE_NAME"], airlines["IATA_CODE"]))

airline_choice = st.sidebar.multiselect(
    "Select Airlines",
    options=list(airline_map.keys())
)

selected_airlines = [airline_map[a] for a in airline_choice]

filtered = df[
    (df['FLIGHT_DATE'] >= pd.to_datetime(date_range[0])) &
    (df['FLIGHT_DATE'] <= pd.to_datetime(date_range[1]))
]

if selected_airlines:
    dim_air = session.table("aviation_project.airlines.DIM_AIRLINES").to_pandas()
    air_sk = dim_air[dim_air["IATA_CODE"].isin(selected_airlines)]["AIRLINE_SK"]
    filtered = filtered[filtered["AIRLINE_SK"].isin(air_sk)]

# 🔥 TABS
tab1, tab2 = st.tabs(["📊 Overview Dashboard", "📚 Advanced Analytics"])

# ----------------------------------------------------
# TAB 1 — OVERVIEW DASHBOARD
# ----------------------------------------------------
with tab1:
    st.subheader("📊 Key Metrics")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Flights", len(filtered))
    col2.metric("Delayed Flights", int((filtered['ARRIVAL_DELAY'] > 0).sum()))
    col3.metric("Cancelled Flights", int(filtered['CANCELLED'].sum()))
    col4.metric("Avg Delay (min)", round(filtered['ARRIVAL_DELAY'].mean(), 2))

    st.markdown("---")
    

    # -----------------------------------------
    # ✈️ Top 10 Busiest Routes (with full airport names in tooltip)
    # -----------------------------------------
    
    st.subheader("✈️ Top 10 Busiest Routes")
    
    busiest = (
        filtered.groupby(['ORIGIN_AIRPORT_SK','DESTINATION_AIRPORT_SK'])
        .size()
        .reset_index(name='FLIGHT_COUNT')
        .sort_values(by='FLIGHT_COUNT', ascending=False)
        .head(10)
    )
    
    # Load DIM_AIRPORTS
    dim_air = session.table("aviation_project.airlines.DIM_AIRPORTS").to_pandas()
    
    # Create FULL NAME column
    dim_air["AIRPORT_FULL_NAME"] = (
        dim_air["CITY"] + ", " + dim_air["STATE"] + " (" + dim_air["IATA_CODE"] + ")"
    )
    
    # Merge origin details
    busiest = busiest.merge(
        dim_air[['AIRPORT_SK','IATA_CODE','AIRPORT_FULL_NAME']],
        left_on='ORIGIN_AIRPORT_SK', right_on='AIRPORT_SK', how='left'
    ).rename(columns={'IATA_CODE':'ORIGIN_CODE', 'AIRPORT_FULL_NAME':'ORIGIN_NAME'}) \
     .drop(columns=['AIRPORT_SK'])
    
    # Merge destination details
    busiest = busiest.merge(
        dim_air[['AIRPORT_SK','IATA_CODE','AIRPORT_FULL_NAME']],
        left_on='DESTINATION_AIRPORT_SK', right_on='AIRPORT_SK', how='left'
    ).rename(columns={'IATA_CODE':'DEST_CODE', 'AIRPORT_FULL_NAME':'DEST_NAME'}) \
     .drop(columns=['AIRPORT_SK'])
    
    # Build chart using Altair
    chart = (
        alt.Chart(busiest)
        .mark_bar()
        .encode(
            x=alt.X("ORIGIN_CODE:N", title="Origin Airport Code"),
            y=alt.Y("FLIGHT_COUNT:Q", title="Flight Count"),
            tooltip=[
                alt.Tooltip("ORIGIN_CODE", title="Origin Code"),
                alt.Tooltip("ORIGIN_NAME", title="Origin Airport"),
                alt.Tooltip("DEST_CODE", title="Destination Code"),
                alt.Tooltip("DEST_NAME", title="Destination Airport"),
                alt.Tooltip("FLIGHT_COUNT", title="Flight Count")
            ]
        )
        .properties(height=400)
    )
    
    st.altair_chart(chart, use_container_width=True)

    
    st.markdown("---")
        
    st.subheader("🏆 Airline Delay Comparison")
    
    air_delay = filtered.groupby("AIRLINE_SK")['ARRIVAL_DELAY'].mean().reset_index()
    
    airlines_dim = session.table("aviation_project.airlines.DIM_AIRLINES").to_pandas()
    
    air_delay["AIRLINE_SK"] = air_delay["AIRLINE_SK"].astype(str)
    airlines_dim["AIRLINE_SK"] = airlines_dim["AIRLINE_SK"].astype(str)
    
    air_delay = air_delay.merge(
        airlines_dim[['AIRLINE_SK','AIRLINE_NAME']],
        on="AIRLINE_SK",
        how="left"
    )
    
    # Rename columns for nicer labels
    air_delay = air_delay.rename(columns={
        "AIRLINE_NAME": "Airline",
        "ARRIVAL_DELAY": "Average Arrival Delay (min)"
    })
    
    # Round delay column
    air_delay["Average Arrival Delay (min)"] = air_delay["Average Arrival Delay (min)"].round(2)
    
    st.bar_chart(air_delay, x="Airline", y="Average Arrival Delay (min)")

    st.markdown("---")

    st.subheader("🛫 Route Delay Heatmap")

    # Load Airport dim
    dim_air = session.table("aviation_project.airlines.DIM_AIRPORTS").to_pandas()
    
    # If full name not available, build one
    if "AIRPORT_NAME" not in dim_air.columns:
        dim_air["AIRPORT_NAME"] = dim_air["CITY"] + ", " + dim_air["STATE"]
    
    routes = filtered.groupby(
        ['ORIGIN_AIRPORT_SK','DESTINATION_AIRPORT_SK']
    )['ARRIVAL_DELAY'].mean().reset_index()
    
    # Merge Origin info
    routes = routes.merge(
        dim_air[['AIRPORT_SK','IATA_CODE','AIRPORT_NAME']], 
        left_on='ORIGIN_AIRPORT_SK', 
        right_on='AIRPORT_SK', 
        how='left'
    ).rename(columns={
        'IATA_CODE':'ORIGIN',
        'AIRPORT_NAME':'ORIGIN_FULL'
    })
    
    # Merge Destination info
    routes = routes.merge(
        dim_air[['AIRPORT_SK','IATA_CODE','AIRPORT_NAME']], 
        left_on='DESTINATION_AIRPORT_SK', 
        right_on='AIRPORT_SK', 
        how='left'
    ).rename(columns={
        'IATA_CODE':'DESTINATION',
        'AIRPORT_NAME':'DESTINATION_FULL'
    })
    
    # Prepare dataset for heatmap
    heatmap_df = routes[['ORIGIN','DESTINATION','ARRIVAL_DELAY','ORIGIN_FULL','DESTINATION_FULL']]
    
    # Heatmap with FULL names in tooltip
    heatmap_chart = (
        alt.Chart(heatmap_df)
        .mark_rect()
        .encode(
            x=alt.X("DESTINATION:N", title="Destination Airport"),
            y=alt.Y("ORIGIN:N", title="Origin Airport"),
            color=alt.Color("ARRIVAL_DELAY:Q", title="Avg Delay (min)",
                            scale=alt.Scale(scheme='redyellowgreen')),
            tooltip=[
                alt.Tooltip("ORIGIN_FULL", title="Origin Airport"),
                alt.Tooltip("DESTINATION_FULL", title="Destination Airport"),
                alt.Tooltip("ARRIVAL_DELAY", title="Avg Delay (min)")
            ]
        )
        .properties(height=450, width=900)
    )
    
    st.altair_chart(heatmap_chart, use_container_width=True)

    st.markdown("---")

    st.subheader("❌ Cancellation Insights")
    cancel = filtered.groupby("AIRLINE_SK")['CANCELLED'].sum().reset_index()

    cancel["AIRLINE_SK"] = cancel["AIRLINE_SK"].astype(str)
    cancel = cancel.merge(
        airlines_dim[['AIRLINE_SK','AIRLINE_NAME']], on="AIRLINE_SK", how="left"
    )

    st.bar_chart(cancel, x="AIRLINE_NAME", y="CANCELLED")

# ----------------------------------------------------
# TAB 2 — ADVANCED ANALYTICS
# ----------------------------------------------------
with tab2:

    st.subheader("⛅ Weather Impact on Delays")
    weather = filtered.groupby("AIRLINE_SK")['WEATHER_DELAY'].sum().reset_index()

    weather["AIRLINE_SK"] = weather["AIRLINE_SK"].astype(str)
    weather = weather.merge(
        airlines_dim[['AIRLINE_SK','AIRLINE_NAME']], on="AIRLINE_SK", how="left"
    )
    st.bar_chart(weather, x="AIRLINE_NAME", y="WEATHER_DELAY")

    st.markdown("---")

    st.subheader("📌 Delay Reason Breakdown")
    delay_reasons = filtered[[
        'AIR_SYSTEM_DELAY','SECURITY_DELAY','AIRLINE_DELAY',
        'LATE_AIRCRAFT_DELAY','WEATHER_DELAY'
    ]].sum().reset_index()

    delay_reasons.columns = ["Reason","Delay"]
    st.bar_chart(delay_reasons, x="Reason", y="Delay")

    st.markdown("---")

    st.subheader("🔥 Top 10 Worst Routes")
    
    # Worst routes calculation
    worst_routes = (
        filtered.groupby(['ORIGIN_AIRPORT_SK','DESTINATION_AIRPORT_SK'])['ARRIVAL_DELAY']
        .mean().reset_index()
        .sort_values(by="ARRIVAL_DELAY", ascending=False)
        .head(10)
    )
    
    # ---- Map Origin Airport Details ----
    dim_air['FULL_NAME'] = dim_air['CITY'] + ", " + dim_air['STATE'].fillna("") + " (" + dim_air['IATA_CODE'] + ")"
    
    worst_routes = worst_routes.merge(
        dim_air[['AIRPORT_SK','FULL_NAME']],
        left_on='ORIGIN_AIRPORT_SK',
        right_on='AIRPORT_SK',
        how='left'
    ).rename(columns={'FULL_NAME':'ORIGIN'})
    
    # ---- Map Destination Airport Details ----
    worst_routes = worst_routes.merge(
        dim_air[['AIRPORT_SK','FULL_NAME']],
        left_on='DESTINATION_AIRPORT_SK',
        right_on='AIRPORT_SK',
        how='left'
    ).rename(columns={'FULL_NAME':'DESTINATION'})
    
    # Final chart
    st.bar_chart(worst_routes, x="ORIGIN", y="ARRIVAL_DELAY")

    st.markdown("---")

    st.subheader("📈 Delay Trend Over Time")
    
    trend = filtered.groupby("FLIGHT_DATE")['ARRIVAL_DELAY'].mean().reset_index()
    
    # Rename for nicer axis labels
    trend = trend.rename(columns={
        "FLIGHT_DATE": "Date",
        "ARRIVAL_DELAY": "Average Arrival Delay (min)"
    })
    
    # Convert date to string to FIX axis labels
    trend["Date"] = trend["Date"].dt.strftime("%Y-%m-%d")
    
    # Round delay values
    trend["Average Arrival Delay (min)"] = trend["Average Arrival Delay (min)"].round(2)
    
    st.line_chart(trend, x="Date", y="Average Arrival Delay (min)")