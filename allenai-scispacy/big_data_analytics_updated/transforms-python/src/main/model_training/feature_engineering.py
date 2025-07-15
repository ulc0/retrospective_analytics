from transforms.api import transform, Input, Output


@transform(
    features_and_labels_input=Input("ri.foundry.main.dataset.2d0861d1-d17c-473a-ab21-df2bc8c25ce6"),
    training_output=Output("/1CDP-Training-736bf7/1CDP Training Playground/cdh_sandbox/data/raw/1cdp_sample/housing_training_data"),
    testing_output=Output("/1CDP-Training-736bf7/1CDP Training Playground/cdh_sandbox/data/raw/1cdp_sample/housing_test_data"),
)
def compute(features_and_labels_input, training_output, testing_output):
    # Converts this TransformInput to a PySpark DataFrame
    features_and_labels = features_and_labels_input.dataframe()

    # Randomly split the PySpark dataframe with 80% training data and 20% testing data
    training_data, testing_data = features_and_labels.randomSplit([0.8, 0.2], seed=0)

    # Write training and testing data back to Foundry
    training_output.write_dataframe(training_data)
    testing_output.write_dataframe(testing_data)