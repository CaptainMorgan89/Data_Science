import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load data
file_path = r"C:\Users\agkap\Desktop\Weather_data.csv"
df = pd.read_csv(file_path)

# Convert 'date' column to datetime (MM/DD/YYYY)
df['date'] = pd.to_datetime(df['date'], dayfirst=False)

# Calculate daily average temperature
df['avg_temp'] = (df['min_temp'] + df['max_temp']) / 2

# Create 'year_month' column in 'YYYY-MM' format for grouping
df['year_month'] = df['date'].dt.to_period('M')

# Group by month & calculate average of daily mean temperature
monthly_avg_temp = df.groupby('year_month')['avg_temp'].mean()

print("\nMonthly Average Temperature:\n", monthly_avg_temp)

# Find the hottest and coldest month
hottest_month = monthly_avg_temp.idxmax()
coldest_month = monthly_avg_temp.idxmin()
print(f"\nHottest month: {hottest_month} with {monthly_avg_temp[hottest_month]:.2f} °C")
print(f"Coldest month: {coldest_month} with {monthly_avg_temp[coldest_month]:.2f} °C")

# Plot 1: Line plot of monthly average temperature
plt.figure(figsize=(12,6))
sns.lineplot(x=monthly_avg_temp.index.astype(str), y=monthly_avg_temp.values, marker='o', color='orange')
plt.title('Monthly Average Temperature')
plt.xlabel('Month (Year-Month)')
plt.ylabel('Average Temperature (°C)')
plt.xticks(rotation=45)
plt.grid(True)
plt.tight_layout()
plt.show()

# Plot 2: Histogram of daily average temperature distribution
plt.figure(figsize=(10,6))
sns.histplot(df['avg_temp'], bins=40, kde=True, color='skyblue')
plt.title('Distribution of Daily Average Temperature')
plt.xlabel('Average Temperature (°C)')
plt.ylabel('Frequency')
plt.grid(True)
plt.tight_layout()
plt.show()
