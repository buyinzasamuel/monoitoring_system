import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

st.set_page_config(page_title="IoT Sensor Dashboard", layout="wide")

st.title("🌡️ IoT Temperature & Humidity Dashboard")

# Load data
df = pd.read_csv("sensor_data.csv")

# Convert timestamps
df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")

# Sidebar filters
st.sidebar.header("Filter Options")
start_date = st.sidebar.date_input("Start date", df["Timestamp"].min().date())
end_date = st.sidebar.date_input("End date", df["Timestamp"].max().date())
filtered = df[(df["Timestamp"].dt.date >= start_date) & (df["Timestamp"].dt.date <= end_date)]

# Show summary metrics
col1, col2, col3, col4 = st.columns(4)
col1.metric("Avg Temperature", f"{filtered['Temperature'].mean():.2f} °C")
col2.metric("Avg Humidity", f"{filtered['Humidity'].mean():.1f}%")
col3.metric("Battery Voltage", f"{filtered['Battery'].iloc[-1]:.3f} V")
col4.metric("Last Motion", filtered['Motion'].iloc[-1])

# --- Plot temperature over time ---
st.subheader("Temperature Trend")
fig, ax = plt.subplots()
ax.plot(filtered["Timestamp"], filtered["Temperature"], label="Temperature (°C)")
ax.set_xlabel("Time")
ax.set_ylabel("Temperature (°C)")
ax.legend()

# 👇 Make date labels smaller and tilted
plt.xticks(rotation=45, fontsize=8)

st.pyplot(fig)

# --- Plot humidity over time ---
st.subheader("Humidity Trend")
fig2, ax2 = plt.subplots()
ax2.plot(filtered["Timestamp"], filtered["Humidity"], color="orange", label="Humidity (%)")
ax2.set_xlabel("Time")
ax2.set_ylabel("Humidity (%)")
ax2.legend()

# 👇 Make date labels smaller and tilted
plt.xticks(rotation=45, fontsize=8)

st.pyplot(fig2)

# --- Table view ---
st.subheader("📋 Raw Sensor Data")
st.dataframe(filtered.sort_values("Timestamp", ascending=False))
