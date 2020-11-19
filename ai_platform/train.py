
import numpy as np
from sklearn.externals import joblib
from sklearn.dummy import DummyRegressor
from sklearn.pipeline import Pipeline


X = np.array([1.0])
y = np.array([1.0])

pipeline = Pipeline([
      ('model', DummyRegressor(strategy="constant", constant=1))
    ])

pipeline.fit(X, y)

print(pipeline.predict(X))

# Export the classifier to a file
joblib.dump(pipeline, 'ai_platform/model.joblib')
