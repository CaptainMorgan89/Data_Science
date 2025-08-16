import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# 📥 Load data
file_path = r"C:\Users\agkap\Desktop\hospital_data.csv"
df = pd.read_csv(file_path)

# 👀 Preview data
print("🔹 First rows of the dataset:")
print(df.head())

# 📅 Convert dates to datetime
df['admission_date'] = pd.to_datetime(df['admission_date'])
df['discharge_date'] = pd.to_datetime(df['discharge_date'])

# 🧮 Calculate length of stay (in days)
df['length_of_stay'] = (df['discharge_date'] - df['admission_date']).dt.days

# 🔥 Average length of stay
avg_stay = df['length_of_stay'].mean()
print(f"\n📊 Average length of stay: {avg_stay:.2f} days")

# 📌 Most common diagnoses
common_diagnoses = df['diagnosis'].value_counts().head(10)
print("\n🏥 Top 10 Most Common Diagnoses:")
print(common_diagnoses)

# 🎨 Plot 1: Age distribution of patients
plt.figure(figsize=(10, 6))
sns.histplot(df['age'], bins=20, kde=True, color='skyblue')
plt.title('Patient Age Distribution')
plt.xlabel('Age')
plt.ylabel('Number of Patients')
plt.grid(True)
plt.tight_layout()
plt.show()

# 🎨 Plot 2: Top 10 Diagnoses
plt.figure(figsize=(12, 6))
sns.barplot(x=common_diagnoses.values, y=common_diagnoses.index, palette='viridis')
plt.title('Top 10 Most Common Diagnoses')
plt.xlabel('Number of Cases')
plt.ylabel('Diagnosis')
plt.tight_layout()
plt.show()

# 📅 Group by month of admission
df['year_month'] = df['admission_date'].dt.to_period('M')
monthly_admissions = df['year_month'].value_counts().sort_index()

# 🎨 Plot 3: Monthly Admission Trend
plt.figure(figsize=(12, 6))
sns.lineplot(x=monthly_admissions.index.astype(str), y=monthly_admissions.values, marker='o', color='orange')
plt.title('Monthly Admission Trend')
plt.xlabel('Month')
plt.ylabel('Number of Admissions')
plt.xticks(rotation=45)
plt.grid(True)
plt.tight_layout()
plt.show()
