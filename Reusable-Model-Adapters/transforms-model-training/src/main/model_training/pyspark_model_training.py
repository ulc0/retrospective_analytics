from pyspark.sql import functions as F
from transforms.api import transform, Input, Output
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    RegexTokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer)
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.functions import vector_to_array
from palantir_models.transforms import ModelOutput
from main.model_adapters.pyspark_adapter import pysparkModelAdapter


@transform(
    # out=Output("ri.foundry.main.dataset.4f10c242-fadf-479a-8da3-edd7309218ce"),
    model_output=ModelOutput("ri.models.main.model.2efd0777-3731-43e6-9ca2-6254ee81d16a"),
    train_df=Input("ri.foundry.main.dataset.903680ab-8838-4e9e-b4e4-7894ee6908cc"),
    test_df=Input("ri.foundry.main.dataset.9d8753df-a1d5-4745-904a-7bc367a7cdf3"),
)
def compute(ctx, model_output, train_df, test_df):

    # Step 2: Tokenize and remove stop words
    tokenizer = RegexTokenizer(inputCol='text', outputCol="words", pattern=r"\s+")
    stopwords_remover = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol="filtered_words")

    # Step 4: Create TF-IDF vector
    hashingTF = HashingTF(inputCol=stopwords_remover.getOutputCol(), outputCol="rawFeatures", numFeatures=10000)
    idf = IDF(inputCol=hashingTF.getOutputCol(), outputCol="features")

    # Apply multi-label binarization on the target_label column
    string_index = StringIndexer(inputCol="target_label", outputCol="label")

    # Define the NaiveBayes classifier
    nb_classifier = NaiveBayes(
        featuresCol=idf.getOutputCol(),
        labelCol="label",
        weightCol="weight",
        predictionCol='prediction',
        probabilityCol='probability',
        rawPredictionCol='rawPrediction',
        )

    # Create a pipeline that combines all the preprocessing steps and the classifiers
    pipeline = Pipeline(
        stages=[
            tokenizer,
            stopwords_remover,
            hashingTF,
            idf,
            string_index,
            nb_classifier
            ])

    # Fit the pipeline to your data
    trained_model = pipeline.fit(train_df.dataframe())

    # 2. Wrap the trained model in your custom ModelAdapter
    wrapped_model = pysparkModelAdapter(trained_model)

    # 3. Save the wrapped model to Foundry
    # Foundry will call ModelAdapter.save
    # Change type MAJOR (X.0.0), MINOR (0.X.0), or PATCH (0.0.X)
    model_output.publish(
        model_adapter=wrapped_model
        )

    # # Make predictions
    # predictions = model.transform(test_df)
    # train_predictions = model.transform(train_df)
    # label_mapping = (
    #     train_predictions
    #     .select("target_label", "label")
    #     .dropDuplicates(subset=["target_label"])
    #     .orderBy(F.col("label").asc())
    #     .agg(F.concat_ws(",", F.collect_list("target_label")).alias("array_label"))
    # )

    # label_mapping = F.split(F.lit(label_mapping.collect()[0]["array_label"]), ",")  # noqa

    # # Show the results
    # output = predictions.select(
    #     "idx",
    #     "text",
    #     F.array_remove(
    #         F.transform(
    #             vector_to_array("probability"),
    #             lambda x, y: F.when(x >= (0.3), F.element_at(label_mapping, y+1)).otherwise(F.lit("-1"))
    #             ), "-1"
    #     ).alias("probability"),
    #     F.array_remove(
    #         F.transform(
    #             vector_to_array("probability_mult"),
    #             lambda x, y: F.when(x >= (0.0), F.element_at(label_mapping, y+1)).otherwise(F.lit("-1"))
    #             ), "-1"
    #     ).alias("probability_mult"),
    # )

    # output = output.select(
    #     "idx",
    #     "text",
    #     F.array_intersect("probability_mult", "probability").alias("prediction")
    #     )

    # train_pred = train_predictions.select(
    #     "idx",
    #     "policy_type",
    #     F.array_remove(
    #         F.transform(
    #             vector_to_array("probability"),
    #             lambda x, y: F.when(x >= (0.3), F.element_at(label_mapping, y+1)).otherwise(F.lit("-1"))
    #             ), "-1"
    #     ).alias("probability"),
    #     F.array_remove(
    #         F.transform(
    #             vector_to_array("probability_mult"),
    #             lambda x, y: F.when(x >= (0.0), F.element_at(label_mapping, y+1)).otherwise(F.lit("-1"))
    #             ), "-1"
    #     ).alias("probability_mult"),
    # )

    # train_pred = train_pred.select(
    #     "idx",
    #     "policy_type",
    #     F.array_intersect("probability_mult", "probability").alias("prediction")
    #     )

    # out.write_dataframe(output.dropDuplicates())
    # train_prediction.write_dataframe(train_pred.dropDuplicates())
