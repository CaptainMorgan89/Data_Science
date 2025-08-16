import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# 📥 Load data
file_path = r"C:\Users\agkap\Desktop\Air_Quality_Cities.csv"
df = pd.read_csv(file_path)

# 👀 Preview
print("🔹 First rows of the dataset:\n", df.head())

# 📅 Convert 'date' column to datetime
df['date'] = pd.to_datetime(df['date'], dayfirst=False)

# 🧹 Check for missing values
print("\n🔍 Missing Values:\n", df.isnull().sum())

# 🔥 Calculate average PM2.5 per city
avg_pm25_per_city = df.groupby('city')['pm25'].mean().sort_values(ascending=False)
print("\n🌫️ Average PM2.5 per city:\n", avg_pm25_per_city)

# 📆 Calculate monthly average PM2.5 (Air Quality Index)
df['year_month'] = df['date'].dt.to_period('M')
monthly_aqi = df.groupby(['year_month', 'city'])['pm25'].mean().reset_index()

# Convert year_month to string for plotting
monthly_aqi['year_month'] = monthly_aqi['year_month'].astype(str)

# 📈 Plot 1: Monthly average PM2.5 per city
plt.figure(figsize=(12, 6))
sns.lineplot(data=monthly_aqi, x='year_month', y='pm25', hue='city', marker='o')
plt.title('Monthly Average PM2.5 per City')
plt.xlabel('Month')
plt.ylabel('PM2.5 (μg/m³)')
plt.xticks(rotation=45)
plt.legend(title='City')
plt.grid(True)
plt.tight_layout()
plt.show()

# 📉 Plot 2: Distribution of PM2.5 for all cities
plt.figure(figsize=(10, 6))
sns.histplot(df['pm25'], bins=30, kde=True, color='salmon')
plt.title('Distribution of PM2.5 for All Cities')
plt.xlabel('PM2.5 (μg/m³)')
plt.ylabel('Frequency')
plt.grid(True)
plt.tight_layout()
plt.show()

# 🔗 Plot 3: Correlation between pollutants
plt.figure(figsize=(8, 6))
sns.heatmap(df[['pm25', 'no2', 'so2', 'co', 'o3']].corr(), annot=True, cmap='coolwarm')
plt.title('Correlation Between Pollutants')
plt.tight_layout()
plt.show()

# 🔥 Extra: Scatter plot NO2 vs PM2.5
plt.figure(figsize=(8, 6))
sns.scatterplot(data=df, x='no2', y='pm25', hue='city')
plt.title('NO2 vs PM2.5 per City')
plt.xlabel('NO2 (μg/m³)')
plt.ylabel('PM2.5 (μg/m³)')
plt.legend(title='City')
plt.grid(True)
plt.tight_layout()
plt.show()
