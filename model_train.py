def sklearn_pipeline_model(source_df):

    from sklearn.linear_model import LinearRegression
    from sklearn.preprocessing import PolynomialFeatures
    from sklearn.preprocessing import StandardScaler
    from sklearn.compose import make_column_transformer
    from sklearn.pipeline import Pipeline
    import numpy as np

    from foundry_ml import Model, Stage

    source_df = source_df.na.drop(subset=['Pclass', "Age", "Fare", "Survived"]).toPandas()
    #source_df.toPandas()
    #source_df.dropna(subset=['Pclass', "Age", "Fare", "Survived"])
    X_train = source_df[['Pclass', "Age", "Fare"]]
    y_train = source_df[['Survived']]

    # Create vectorizer
    column_transformer = make_column_transformer(
       ('passthrough', ['Pclass', "Age", "Fare"])
    )
    # Vectorizer -> StandardScaler -> PolynomialExpansion -> LinearRegression
    pipeline = Pipeline([
            ("preprocessing", column_transformer),
            ("ss", StandardScaler()),
            ("pf", PolynomialFeatures(degree=2)),
            ("lr", LinearRegression())
       ])

    pipeline.fit(X_train, y_train)

    # Expand out pipeline
    # can be also created via list comprehension:  Model(*[Stage(s[1]) for s in pipeline.steps])
    model = Model( Stage(pipeline["preprocessing"]), 
                    Stage(pipeline["ss"], output_column_name="features"), 
                    Stage(pipeline["pf"], output_column_name="features"), 
                    Stage(pipeline["lr"]))
                    
    return model
