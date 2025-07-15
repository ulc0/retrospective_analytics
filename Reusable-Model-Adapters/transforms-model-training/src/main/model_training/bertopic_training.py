from transforms.api import transform, Input, Output, configure
from palantir_models.models import ModelVersionChangeType
from palantir_models.transforms import ModelOutput
from main.model_adapters.bertopic_adapter import BERTopicModelAdapter
from sentence_transformers import SentenceTransformer
from bertopic import BERTopic
from zipfile import ZipFile
import tempfile
import pandas as pd
import os
import shutil
import base64
from bertopic.vectorizers import ClassTfidfTransformer
from bertopic.dimensionality import BaseDimensionalityReduction
from sklearn.linear_model import LogisticRegression


def download_file(filesystem, input_file_path, local_file_path=None, base64_decode=False):
    """
    Download a file from a Foundry dataset to the local filesystem.
    If the input_file_path is None, a temporary file is created, which you must delete yourself after using it.
    :param filesystem: an instance of transform.api.FileSystem
    :param input_file_path: logical path on the Foundry dataset to download from
    :param local_file_path: path of the file to download to on the local file system (default=None)
    :base64_encode: if set to True, decode data using base64 (default=False)
    :return: str path of the downloaded file on the local file system
    """

    # Check if a different temp directory is specified in the Spark environment, and use it if so
    TEMPDIR_OVERRIDE = os.getenv('FOUNDRY_PYTHON_TEMPDIR')
    tempfile.tempdir = TEMPDIR_OVERRIDE if TEMPDIR_OVERRIDE is not None else tempfile.tempdir

    if local_file_path is None:
        _, local_file_path = tempfile.mkstemp()

    if base64_decode:
        _, download_file_path = tempfile.mkstemp()
    else:
        download_file_path = local_file_path

    try:
        with filesystem.open(input_file_path, 'rb') as f_in, open(download_file_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

        if base64_decode:
            with open(download_file_path, 'rb') as fin, open(local_file_path, 'wb') as fout:
                base64.decode(fin, fout)

        return local_file_path

    finally:
        if base64_decode:
            os.remove(download_file_path)


def train_model(df, raw_model):
    """
        This function contains your custom training logic.
    """
    # Load zip file of sentence transformer model from dataset filesystem to temp directory
    # Available models: https://public.ukp.informatik.tu-darmstadt.de/reimers/sentence-transformers/v0.2/
    temp_dir = tempfile.mkdtemp()
    model_package = download_file(
        raw_model.filesystem(),
        "all-MiniLM-L6-v2.zip",
        "{}/all-MiniLM-L6-v2.zip".format(temp_dir)
    )
    with ZipFile(model_package, "r") as zObject:
        # Extract all files from zip and put them in temp directory
        temp_dir2 = tempfile.mkdtemp()
        zObject.extractall(path=temp_dir2)

        # Load the SentenceTransformer model from extracted files
        embedding_model = SentenceTransformer(temp_dir2)

        # Skip over dimensionality reduction, replace cluster model with classifier,
        # and reduce frequent words while we are at it.
        empty_dimensionality_model = BaseDimensionalityReduction()
        clf = LogisticRegression()
        ctfidf_model = ClassTfidfTransformer(reduce_frequent_words=True)

        # Load BERTopic with offline SentenceTransformer model
        # Create a fully supervised BERTopic instance
        model = BERTopic(
                embedding_model=embedding_model,
                umap_model=empty_dimensionality_model,
                hdbscan_model=clf,
                ctfidf_model=ctfidf_model,
                verbose=True
        )

    zObject.close()
    topics, probs = model.fit_transform(df['text_clean'].astype(str).tolist(), y=df['label'].to_numpy())

    return model


@transform(
    training_data=Input("ri.foundry.main.dataset.1014fb29-bce7-4041-8db2-0d231d3362d0"),
    model_output=ModelOutput("ri.models.main.model.bfe5e3a1-45c2-4cc5-ad8e-9254fb440da0"),
    output=Output("ri.foundry.main.dataset.7d3d6226-a29d-4a57-a850-2b546e6ac5e2"),  # to enable Preview
    raw_model=Input("ri.foundry.main.dataset.72915da4-0e71-458c-893d-96f630eba9ba"),
)
def compute(training_data, raw_model, model_output, output):
    '''
        This function contains logic to read and write to Foundry.
    '''
    df = training_data.pandas()[['text_clean', 'policy_type']]
    df = df[(df.policy_type != "['none']") | (~df.text_clean.isna())]

    # Remove brackets and quotation marks from the policy_type column
    df['label'] = df['policy_type'].str.replace('[', '').str.replace(']', '').str.replace('\'', '')

    # Split the values in the policy_type column by comma and spread each value to a separate row
    df = df.assign(label=df['label'].str.split(',')).explode('label')

    df['label'] = df.label.astype('category').cat.codes

    # 1. Train the model in a Python transform
    trained_model = train_model(df, raw_model)

    # 2. Wrap the trained model in your custom ModelAdapter
    wrapped_model = BERTopicModelAdapter(trained_model)

    # 3. Save the wrapped model to Foundry
    # Foundry will call ModelAdapter.save
    # Change type MAJOR (X.0.0), MINOR (0.X.0), or PATCH (0.0.X)
    model_output.publish(
        model_adapter=wrapped_model,
        change_type=ModelVersionChangeType.MINOR
    )

    output.write_pandas(trained_model.get_topic_info())  # to enable Preview
