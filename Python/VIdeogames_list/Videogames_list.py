import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import poisson

# 📥 Load dataset
file_path = r"C:\Users\agkap\Desktop\Videogames_List.csv"
try:
    df = pd.read_csv(file_path, encoding='utf-8-sig')  # handles BOM if present
except UnicodeDecodeError:
    df = pd.read_csv(file_path, encoding='latin1')  # fallback for weird encodings

# 🧹 Clean column names from trailing spaces, semicolons, etc.
df.columns = df.columns.str.strip().str.replace(';', '', regex=False)


# 👀 Preview first rows
print("🔹 First rows of dataset:")
print(df.head())

# 🔄 Convert sales columns to numeric (clean stray text)
sales_cols = ['NA_Sales', 'EU_Sales', 'JP_Sales', 'Other_Sales', 'Global_Sales']
for col in sales_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# 🧹 Drop rows with missing Year or Global_Sales
df = df.dropna(subset=['Year', 'Global_Sales'])

# 🔢 Convert Year to int
df['Year'] = df['Year'].astype(int)

# Καθαρισμός ονομάτων
df['Name'] = df['Name'].str.strip().str.replace(';', '', regex=False)

# Top-selling games
top_games = df.sort_values('Global_Sales', ascending=False).head(10)
print("\n🏆 Top 10 Best-Selling Games:\n", top_games[['Name', 'Global_Sales']])

# Plot
plt.figure(figsize=(12, 6))
sns.barplot(x='Global_Sales', y='Name', data=top_games, palette='magma')
plt.title('Top 10 Best-Selling Video Games of All Time')
plt.xlabel('Global Sales (Millions)')
plt.ylabel('Game Title')
plt.tight_layout()
plt.show()


# ------------------------------------------
# 2️⃣ Most Popular Platform
# ------------------------------------------
platform_sales = df.groupby('Platform')['Global_Sales'].sum().sort_values(ascending=False)
print("\n Total Sales by Platform:\n", platform_sales)

plt.figure(figsize=(12, 6))
sns.barplot(x=platform_sales.index, y=platform_sales.values, palette='cool')
plt.title(' Total Global Sales by Platform')
plt.xlabel('Platform')
plt.ylabel('Global Sales (Millions)')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# ------------------------------------------
# 3️⃣ Most Popular Genre
# ------------------------------------------
genre_sales = df.groupby('Genre')['Global_Sales'].sum().sort_values(ascending=False)
print("\n Total Sales by Genre:\n", genre_sales)

plt.figure(figsize=(10, 6))
sns.barplot(x=genre_sales.index, y=genre_sales.values, palette='viridis')
plt.title(' Total Global Sales by Genre')
plt.xlabel('Genre')
plt.ylabel('Global Sales (Millions)')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# ------------------------------------------
# 4️⃣ Sales Variation by Region
# ------------------------------------------
regions = ['NA_Sales', 'EU_Sales', 'JP_Sales', 'Other_Sales']
region_totals = df[regions].sum()
print("\n Total Sales by Region:\n", region_totals)

plt.figure(figsize=(8, 8))
plt.pie(region_totals, labels=region_totals.index, autopct='%1.1f%%', colors=sns.color_palette('Set2'))
plt.title(' Regional Sales Distribution')
plt.show()

# ------------------------------------------
# 5️⃣ Year of Release vs Sales
# ------------------------------------------
yearly_sales = df.groupby('Year')['Global_Sales'].sum()

plt.figure(figsize=(12, 6))
sns.lineplot(x=yearly_sales.index, y=yearly_sales.values, marker='o', color='crimson')
plt.title('Global Sales by Year of Release')
plt.xlabel('Year')
plt.ylabel('Global Sales (Millions)')
plt.grid(True)
plt.tight_layout()
plt.show()

# ------------------------------------------
# 6️⃣ Publisher vs Sales
# ------------------------------------------
publisher_sales = df.groupby('Publisher')['Global_Sales'].sum().sort_values(ascending=False).head(10)
print("\n Top 10 Publishers by Global Sales:\n", publisher_sales)

plt.figure(figsize=(12, 6))
sns.barplot(x=publisher_sales.values, y=publisher_sales.index, palette='plasma')
plt.title(' Top 10 Publishers by Global Sales')
plt.xlabel('Global Sales (Millions)')
plt.ylabel('Publisher')
plt.tight_layout()
plt.show()

# ------------------------------------------
# 7️⃣ Poisson Distribution on Sales Counts
# ------------------------------------------
# Count how many games sold X million copies
sales_counts = df['Global_Sales'].round().value_counts().sort_index()
print("\nSales Count Distribution:\n", sales_counts)

# Fit Poisson distribution
lambda_poisson = sales_counts.mean()
x = sales_counts.index
poisson_probs = poisson.pmf(x, lambda_poisson) * sales_counts.sum()

# Plot actual vs Poisson model
plt.figure(figsize=(10, 6))
plt.bar(x, sales_counts.values, label='Actual Data', alpha=0.6, color='skyblue')
plt.plot(x, poisson_probs, 'ro-', label=f'Poisson (λ={lambda_poisson:.2f})')
plt.title(' Poisson Fit to Video Game Sales Counts')
plt.xlabel('Global Sales (Millions, Rounded)')
plt.ylabel('Number of Games')
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()
