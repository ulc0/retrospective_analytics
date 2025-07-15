# from sklearn.base import BaseEstimator, TransformerMixin


# class FeatureEngineeringTransformer(BaseEstimator, TransformerMixin):
#     def fit(self, X, y=None):
#         return self

#     def transform(self, X):
#         # Replace this with your feature_engineering function logic
#         schema = [
#             "USMER", "MEDICAL_UNIT", "SEX", "PATIENT_TYPE", "INTUBED", "PNEUMONIA", "AGE", "PREGNANT",
#             "DIABETES", "COPD", "ASTHMA", "INMSUPR", "HIPERTENSION", "OTHER_DISEASE", "CARDIOVASCULAR",
#             "OBESITY", "RENAL_CHRONIC", "TOBACCO", "ICU"
#             ]

#         data = X[schema]
#         return data


def feature_engineering(X):
    # Replace this with your feature_engineering function logic
    schema = [
        "USMER", "MEDICAL_UNIT", "SEX", "PATIENT_TYPE", "INTUBED", "PNEUMONIA", "AGE", "PREGNANT",
        "DIABETES", "COPD", "ASTHMA", "INMSUPR", "HIPERTENSION", "OTHER_DISEASE", "CARDIOVASCULAR",
        "OBESITY", "RENAL_CHRONIC", "TOBACCO", "ICU"
        ]

    data = X[schema]
    return data
