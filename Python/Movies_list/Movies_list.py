import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.ticker as ticker

# 📥 Load CSV
file_path = r"C:\Users\agkap\Desktop\Movies_list.csv"
df = pd.read_csv(file_path)

# 🧹 Data Cleaning
df.dropna(inplace=True)
df['budget'] = pd.to_numeric(df['budget'], errors='coerce')
df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')
df.dropna(subset=['budget', 'revenue'], inplace=True)
df['profit'] = df['revenue'] - df['budget']

# 📦 Formatter for millions
def millions(x, pos):
    return f'{x*1e-6:.0f}M'
formatter = ticker.FuncFormatter(millions)

# 🎨 Histogram for Profit in millions
plt.figure(figsize=(10, 6))
plt.hist(df['profit'], bins=30, color='purple', edgecolor='black')
plt.title('Histogram of Movie Profits (in Millions $)')
plt.xlabel('Profit (Millions $)')
plt.ylabel('Number of Movies')
plt.gca().xaxis.set_major_formatter(formatter)
plt.grid(True)
plt.tight_layout()
plt.show()

# 🎨 Scatter Plot: Budget vs Revenue with break-even line
plt.figure(figsize=(10, 6))
sns.scatterplot(x='budget', y='revenue', data=df, hue='genre', palette='Set2')
plt.title('Budget vs Revenue (in Millions $)')
plt.xlabel('Budget (Millions $)')
plt.ylabel('Revenue (Millions $)')

# 🔥 Add red Break Even line (Revenue = Budget)
max_value = max(df['budget'].max(), df['revenue'].max())
plt.plot([0, max_value], [0, max_value], color='red', linestyle='--', linewidth=2, label='Break Even (Revenue = Budget)')

# 📦 Axis formatting
plt.gca().xaxis.set_major_formatter(formatter)
plt.gca().yaxis.set_major_formatter(formatter)

plt.legend(title='Genre')
plt.grid(True)
plt.tight_layout()
plt.show()
