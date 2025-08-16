import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import GaussianNB
from sklearn.metrics import accuracy_score

# Data: [Age, Salary]
X = [
    [22, 20000],
    [25, 22000],
    [47, 50000],
    [52, 52000],
    [46, 49000],
    [56, 60000],
    [23, 23000],
    [24, 25000],
    [48, 52000],
    [50, 51000],
]

# Labels: 1 = purchased, 0 = did not purchase
y = [0, 0, 1, 1, 1, 1, 0, 0, 1, 1]

# Split into train/test sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=0)

# Create & train model
model = GaussianNB()
model.fit(X_train, y_train)

# Prediction
y_pred = model.predict(X_test)

# Evaluation
print("Accuracy:", accuracy_score(y_test, y_pred))

# Test new person: 28 years old, salary 24000
new_person = [[28, 24000]]
prediction = model.predict(new_person)
print("Will purchase?" if prediction[0] == 1 else "Will not purchase")

# --- PLOT ---

# Prepare data for plotting
import pandas as pd
df = pd.DataFrame(X, columns=["Age", "Salary"])
df["Purchased"] = y

# Add new person to the plot
df_new = pd.DataFrame([[28, 24000, prediction[0]]], columns=["Age", "Salary", "Purchased"])
df_all = pd.concat([df, df_new], ignore_index=True)

# Plot using seaborn
plt.figure(figsize=(8, 6))
sns.scatterplot(data=df_all, x="Age", y="Salary", hue="Purchased", palette={0: "red", 1: "green"}, s=100)

# Highlight new person
plt.scatter(28, 24000, color="blue", s=200, edgecolor="black", marker="X", label="New Person")

plt.title("Purchase Decision Based on Age & Salary")
plt.legend()
plt.grid(True)
plt.show()
