# from transforms.api import transform, Input, Output
# from palantir_models.transforms import ModelInput


# @transform(
#     training_data=Input("ri.foundry.main.dataset.1014fb29-bce7-4041-8db2-0d231d3362d0"),
#     model_input=ModelInput("ri.models.main.model.bfe5e3a1-45c2-4cc5-ad8e-9254fb440da0"),
#     output=Output("/DCIPHER/Ecosystem/3_Modular Workflows/Modeling/Code Workspace/BERTopic/models/prediction_data"),  # to enable Preview
# )
# def compute(training_data, model_input, output):
#     '''
#         This function contains logic to read and write to Foundry.
#     '''
#     df = training_data.pandas()[['text_clean', 'policy_type']]
#     df = df[(df.policy_type == "['none']") & (~df.text_clean.isna())]

#     # Remove brackets and quotation marks from the policy_type column
#     df['label'] = df['policy_type'].str.replace('[', '').str.replace(']', '').str.replace('\'', '')

#     # Split the values in the policy_type column by comma and spread each value to a separate row
#     df = df.assign(label=df['label'].str.split(',')).explode('label')

#     df['label'] = df.label.astype('category').cat.codes

#     topics, probs = model_input.transform(df)
#     # This assumes your model adapter returns a dataframe "df_out"
#     output.write_pandas(inference_outputs.df_out)
