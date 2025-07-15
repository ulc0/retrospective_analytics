"""
    This is an example implementation of splitting input labeled data into testing and training datasets in Foundry.
"""

"""
from transforms.api import transform, Input, Output


@transform(
    features_and_labels_input=Input("/DCIPHER/Ecosystem/data/FEATURES_AND_LABELS_DATASET"),
    training_output=Output("/DCIPHER/Ecosystem/data/TRAIN_DATASET"),
    testing_output=Output("/DCIPHER/Ecosystem/data/TEST_DATASET"),
)
def compute(features_and_labels_input, training_output, testing_output):
    features_and_labels = features_and_labels_input.dataframe()

    training_data, testing_data = features_and_labels.randomSplit([0.8, 0.2], seed=0)
    training_output.write_dataframe(training_data)
    testing_output.write_dataframe(testing_data)
"""
