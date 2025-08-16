import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.ticker as mtick

# 📂 Load data
file_path = r"C:\Users\agkap\Desktop\finance_liquor_sales.csv"
df = pd.read_csv(file_path)

# 🧹 Data Cleaning
# ----------------------------------------------------
# Remove rows with missing values in key columns
df = df.dropna(subset=['date', 'zip_code', 'store_name', 'item_description', 'sale_dollars'])

# Convert 'date' to datetime and filter for 2016–2019
df['date'] = pd.to_datetime(df['date'], errors='coerce')
df = df.dropna(subset=['date'])
df = df[(df['date'].dt.year >= 2016) & (df['date'].dt.year <= 2019)]

# Clean zip_code (convert to string + pad with zeros)
df['zip_code'] = df['zip_code'].astype(str).str.zfill(5)

# Convert sale_dollars to numeric
df['sale_dollars'] = pd.to_numeric(df['sale_dollars'], errors='coerce')
df = df.dropna(subset=['sale_dollars'])

# 📊 Analysis
# ----------------------------------------------------
# 1️⃣ Most popular item per zip_code
popular_item_zip = (
    df.groupby(['zip_code', 'item_description'])['sale_dollars']
    .sum()
    .reset_index()
    .sort_values(['zip_code', 'sale_dollars'], ascending=[True, False])
    .drop_duplicates(subset=['zip_code'], keep='first')
    .rename(columns={
        'item_description': 'top_item',
        'sale_dollars': 'total_sales'
    })
)

print("📌 Most Popular Item per Zip Code (2016–2019):\n", popular_item_zip)

# 2️⃣ Sales percentage per store
store_sales = (
    df.groupby('store_name')['sale_dollars']
    .sum()
    .reset_index()
    .rename(columns={'sale_dollars': 'total_sales'})
)
total_sales_all = store_sales['total_sales'].sum()
store_sales['sales_percentage'] = (store_sales['total_sales'] / total_sales_all) * 100

print("\n📊 Sales Percentage per Store:\n", store_sales.sort_values('sales_percentage', ascending=False))

# 🎨 Visualization
# ----------------------------------------------------
plt.figure(figsize=(12, 8))
sns.barplot(
    x='sales_percentage',
    y='store_name',
    data=store_sales.sort_values('sales_percentage', ascending=False).head(15),
    palette='coolwarm'
)
plt.title('Top 15 Stores by Sales Percentage (2016–2019)', fontsize=16)
plt.xlabel('Sales Percentage (%)')
plt.ylabel('Store Name')
plt.gca().xaxis.set_major_formatter(mtick.PercentFormatter(decimals=1))
plt.tight_layout()
plt.show()
