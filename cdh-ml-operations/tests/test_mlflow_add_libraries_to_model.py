        # Create and log a model to the Model Registry

        import pandas as pd
        from sklearn import datasets
        from sklearn.ensemble import RandomForestClassifier
        import mlflow
        import mlflow.sklearn
        from mlflow.models import infer_signature

        with mlflow.start_run() as run:
            iris = datasets.load_iris()
            iris_train = pd.DataFrame(iris.data, columns=iris.feature_names)
            clf = RandomForestClassifier(max_depth=7, random_state=0)
            clf.fit(iris_train, iris.target)
            signature = infer_signature(iris_train, clf.predict(iris_train))
            mlflow.sklearn.log_model(
                clf, "iris_rf", signature=signature, registered_model_name="model-with-libs"
            )

        # model uri for the above model
        model_uri = "models:/model-with-libs/1"

        # Import utility
        from mlflow.models.utils import add_libraries_to_model

        # Log libraries to the original run of the model
        add_libraries_to_model(model_uri)

        # Log libraries to some run_id
        existing_run_id = run.info.run_id #"21df94e6bdef4631a9d9cb56f211767f"
        add_libraries_to_model(model_uri, run_id=existing_run_id)

        # Log libraries to a new run
        with mlflow.start_run():
            add_libraries_to_model(model_uri)

        # Log libraries to a new registered model named 'new-model'
        with mlflow.start_run():
            add_libraries_to_model(model_uri, registered_model_name="new-model")
        