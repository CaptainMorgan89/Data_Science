import pandas as pd
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt
import seaborn as sns

# Data
data = {
    'Name': ['John', 'Jane', 'Jack', 'Jill', 'Joe', 'Jim', 'Jerry', 'Jessica', 'Joyce', 'Jake',
             'Julie', 'Jordan', 'Jasmine', 'Jeff', 'Javier', 'Jackson', 'Joshua', 'Judy', 'Jonathan', 'Jayden'],
    'Occupation': ['Teacher', 'Doctor', 'Engineer', 'Teacher', 'Engineer', 'Teacher', 'Doctor', 'Engineer',
                   'Doctor', 'Teacher', 'Teacher', 'Teacher', 'Engineer', 'Teacher', 'Teacher', 'Teacher',
                   'Nurse', 'Teacher', 'Engineer', 'Teacher'],
    'Age': [34, 36, 29, 22, 29, 45, 52, 31, 38, 47, 30, 28, 26, 40, 42, 46, 38, 49, 37, 42]
}

df = pd.DataFrame(data)

# Filter only for Teachers
teachers = df[df['Occupation'] == 'Teacher']

# Calculate average age
average_age = np.mean(teachers['Age'])
print(f"The average age of teachers is {average_age:.2f} years.")

# Normality test
k2, p = stats.normaltest(teachers['Age'])
alpha = 0.05
print(f"p = {p:.5f}")

if p < alpha:
    print("❌ The null hypothesis can be rejected; the age distribution is NOT normal.")
else:
    print("✅ The null hypothesis cannot be rejected; the age distribution is NORMAL.")

# 📊 Visualization: Histogram + Boxplot
plt.figure(figsize=(12, 6))

plt.subplot(1, 2, 1)
sns.histplot(teachers['Age'], kde=True, color='skyblue', bins=8, edgecolor='black')
plt.title('Age Distribution of Teachers')
plt.xlabel('Age')
plt.ylabel('Frequency')
plt.text(teachers['Age'].min(), 2, f'p-value = {p:.5f}', fontsize=12, color='red')

plt.subplot(1, 2, 2)
sns.boxplot(y=teachers['Age'], color='lightgreen')
plt.title('Boxplot of Teachers\' Age')
plt.ylabel('Age')

plt.tight_layout()
plt.show()
