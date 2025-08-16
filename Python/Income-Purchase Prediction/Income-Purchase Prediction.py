import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression, LogisticRegression

# 🔥 Data: Customer income and whether they purchased (1) or not (0)
X = np.array([15000, 30000, 45000, 60000, 75000]).reshape(-1, 1)
y = np.array([0, 0, 1, 1, 1])

# 📐 Linear Regression
linear_model = LinearRegression()
linear_model.fit(X, y)
y_linear_pred = linear_model.predict(X)

# 📐 Logistic Regression
logistic_model = LogisticRegression()
logistic_model.fit(X, y)
X_test = np.linspace(10000, 80000, 300).reshape(-1, 1)
y_logistic_prob = logistic_model.predict_proba(X_test)[:, 1]

# 📊 Plot
plt.figure(figsize=(10, 6))
plt.scatter(X, y, color="black", label="Data Points")
plt.plot(X, y_linear_pred, color="blue", label="Linear Regression")
plt.plot(X_test, y_logistic_prob, color="red", label="Logistic Regression (Sigmoid)")
plt.title("Linear vs Logistic Regression")
plt.xlabel("Customer Income (€)")
plt.ylabel("Prediction")
plt.legend()
plt.grid(True)
plt.show()
