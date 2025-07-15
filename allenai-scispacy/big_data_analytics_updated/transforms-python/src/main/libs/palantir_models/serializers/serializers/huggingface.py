import importlib

from palantir_models import ModelSerializer, ModelStateReader, ModelStateWriter


class HfPipelineSerializer(ModelSerializer):
    """
    Serializer for huggingface transformers pipelines.
    Allows setting the pipeline task (e.g. sentiment-analysis).
    """

    DIR_NAME = "pipeline"

    def __init__(self, pipeline_type, **load_kwargs):
        self.transformers = importlib.import_module("transformers")
        self.pipeline_type = pipeline_type
        self.load_kwargs = load_kwargs

    def serialize(self, writer: ModelStateWriter, obj):
        pipeline_dir = writer.mkdir(self.DIR_NAME)
        obj.save_pretrained(pipeline_dir)

    def deserialize(self, reader: ModelStateReader):
        pipeline_dir = reader.dir(self.DIR_NAME)
        return self.transformers.pipeline(self.pipeline_type, model=pipeline_dir, **self.load_kwargs)


class HfAutoModelSerializer(ModelSerializer):
    """
    Serializer for huggingface transformers AutoModel classes, using the
    from_pretrained and save_pretrained methods.
    Allows configuring a specific subclass (e.g. AutoModelForSequenceClassification or
    BertForTokenClassification) and passing additional kwargs to from_pretrained
    (e.g. num_labels=2).
    """

    DIR_NAME = "model"

    def __init__(self, model_class=None, **load_kwargs):
        if model_class is None:
            transformers = importlib.import_module("transformers")
            model_class = transformers.AutoModel
        self.model_class = model_class
        self.load_kwargs = load_kwargs

    def serialize(self, writer: ModelStateWriter, obj):
        model_dir = writer.mkdir(self.DIR_NAME)
        obj.save_pretrained(model_dir)

    def deserialize(self, reader: ModelStateReader):
        model_dir = reader.dir(self.DIR_NAME)
        return self.model_class.from_pretrained(model_dir, **self.load_kwargs)


class HfAutoTokenizerSerializer(ModelSerializer):
    """
    Serializer for huggingface transformers AutoTokenizer.
    """

    DIR_NAME = "tokenizer"

    def __init__(self, tokenizer_class=None, **load_kwargs):
        if tokenizer_class is None:
            transformers = importlib.import_module("transformers")
            tokenizer_class = transformers.AutoTokenizer
        self.tokenizer_class = tokenizer_class
        self.load_kwargs = load_kwargs

    def serialize(self, writer: ModelStateWriter, obj):
        tokenizer_dir = writer.mkdir(self.DIR_NAME)
        obj.save_pretrained(tokenizer_dir)

    def deserialize(self, reader: ModelStateReader):
        tokenizer_dir = reader.dir(self.DIR_NAME)
        return self.tokenizer_class.from_pretrained(tokenizer_dir, **self.load_kwargs)


class HfAutoProcessorSerializer(ModelSerializer):
    """
    Serializer for huggingface transformers AutoProcessor.
    """

    DIR_NAME = "processor"

    def __init__(self, processor_class=None, **load_kwargs):
        if processor_class is None:
            transformers = importlib.import_module("transformers")
            processor_class = transformers.AutoProcessor
        self.processor_class = processor_class
        self.load_kwargs = load_kwargs

    def serialize(self, writer: ModelStateWriter, obj):
        processor_dir = writer.mkdir(self.DIR_NAME)
        obj.save_pretrained(processor_dir)

    def deserialize(self, reader: ModelStateReader):
        processor_dir = reader.dir(self.DIR_NAME)
        return self.processor_class.from_pretrained(processor_dir, **self.load_kwargs)
