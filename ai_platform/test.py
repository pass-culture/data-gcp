
import numpy as np
from sklearn.externals import joblib


X = np.array([1.0])

model = joblib.load('ai_platform/model.joblib')

print(
    model.predict(X)
)
