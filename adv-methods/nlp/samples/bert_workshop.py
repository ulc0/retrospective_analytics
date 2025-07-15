# Databricks notebook source
# MAGIC %md
# MAGIC # 🤗 Train a BERT Model From Scratch
# MAGIC
# MAGIC ## **Module 2: Pre-Training**

# COMMAND ----------

# MAGIC %md
# MAGIC Here, we'll walk through using Composer to load a Hugging Face BERT model and pre-train it on the Colossal Clean Crawled Corpus (C4) dataset from [Common Crawl](https://commoncrawl.org/).
# MAGIC
# MAGIC ### Recommended Background
# MAGIC
# MAGIC This tutorial assumes you are familiar with transformer models for NLP and with Hugging Face.
# MAGIC
# MAGIC To better understand the Composer part, make sure you're comfortable with the material in our [Getting Started][getting_started] tutorial.
# MAGIC
# MAGIC ### Tutorial Goals and Concepts Covered
# MAGIC
# MAGIC The goal of this tutorial is to demonstrate how to pre-train a Hugging Face transformer using the Composer library!
# MAGIC
# MAGIC In the next module, we will walk through a [tutorial][huggingface_models] on fine-tuning a pretrained BERT-base model. After both the pre-training and fine-tuning, the BERT model should be able to determine if a sentence has positive or negative sentiment.
# MAGIC
# MAGIC Along the way, we will touch on:
# MAGIC
# MAGIC * Creating our Hugging Face BERT model, tokenizer, and data loaders
# MAGIC * Wrapping the Hugging Face model as a `ComposerModel` for use with the Composer trainer
# MAGIC * Training with Composer
# MAGIC * Visualization examples
# MAGIC
# MAGIC Let's do this 🚀
# MAGIC
# MAGIC [getting_started]: https://docs.mosaicml.com/en/stable/examples/getting_started.html
# MAGIC [huggingface_models]: https://docs.mosaicml.com/en/stable/examples/huggingface_models.html

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Composer
# MAGIC
# MAGIC To use Hugging Face with Composer, we'll need to install Composer *with the NLP dependencies*. If you haven't already, run: 

# COMMAND ----------

# MAGIC %pip install -U pip
# MAGIC %pip install -U 'mosaicml[nlp, streaming]==0.10.1'
# MAGIC # To install from source instead of the last release, comment the command above and uncomment the following one.
# MAGIC # %pip install 'mosaicml[nlp, tensorboard] @ git+https://github.com/mosaicml/composer.git'"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Hugging Face Model
# MAGIC First, we import a BERT model (specifically, BERT-base for uncased text) and its associated tokenizer from the transformers library.

# COMMAND ----------

import transformers

# Create a BERT sequence classification model using Hugging Face transformers
config = transformers.AutoConfig.from_pretrained('bert-base-uncased')
model = transformers.AutoModelForMaskedLM.from_config(config)
tokenizer = transformers.AutoTokenizer.from_pretrained('bert-base-uncased')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Dataloaders

# COMMAND ----------

# MAGIC %md
# MAGIC Next, we will download and tokenize the C4 datasets. 

# COMMAND ----------

from composer.datasets import StreamingC4
from multiprocessing import cpu_count

# Tokenize the C4 dataset
train_dataset = StreamingC4(remote='s3://mosaicml-internal-temporary-202210-ocwdemo/mds/1-gz', 
                                    local='/tmp/c4local',
                                    shuffle=True,
                                    max_seq_len=128,
                                    split='train', 
                                    tokenizer_name='bert-base-uncased')
eval_dataset = StreamingC4(remote='s3://mosaicml-internal-temporary-202210-ocwdemo/mds/1-gz',
                                    local='/tmp/c4local',
                                    shuffle=True,
                                    max_seq_len=128,
                                    split='val',
                                    tokenizer_name='bert-base-uncased')

# COMMAND ----------

# MAGIC %md
# MAGIC Here, we will create a PyTorch `DataLoader` for each of the datasets generated in the previous block.

# COMMAND ----------

from torch.utils.data import DataLoader
data_collator = transformers.DataCollatorForLanguageModeling(tokenizer=tokenizer, mlm=True)
# data_collator = transformers.DefaultDataCollator(return_tensors='pt')
train_dataloader = DataLoader(train_dataset, batch_size=16, shuffle=False, drop_last=False, collate_fn=data_collator)
eval_dataloader = DataLoader(eval_dataset,batch_size=16, shuffle=False, drop_last=False, collate_fn=data_collator)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert model to `ComposerModel`
# MAGIC
# MAGIC Composer uses `HuggingFaceModel` as a convenient interface for wrapping a Hugging Face model (such as the one we created above) in a `ComposerModel`. Its parameters are:
# MAGIC
# MAGIC - `model`: The Hugging Face model to wrap.
# MAGIC - `metrics`: A list of torchmetrics to apply to the output of `validate` (a `ComposerModel` method).
# MAGIC - `use_logits`: A boolean which, if True, flags that the model's output logits should be used to calculate validation metrics.
# MAGIC
# MAGIC See the [API Reference][api] for additional details.
# MAGIC
# MAGIC [api]: https://docs.mosaicml.com/en/stable/api_reference/generated/composer.models.HuggingFaceModel.html

# COMMAND ----------

from torchmetrics.collections import MetricCollection
from composer.models.huggingface import HuggingFaceModel
from composer.metrics import LanguageCrossEntropy, MaskedAccuracy

metrics = [LanguageCrossEntropy(vocab_size=tokenizer.vocab_size), MaskedAccuracy(ignore_index=-100)]
# Package as a trainer-friendly Composer model
composer_model = HuggingFaceModel(model, metrics=metrics, use_logits=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimizers and Learning Rate Schedulers

# COMMAND ----------

# MAGIC %md
# MAGIC The last setup step is to create an optimizer and a learning rate scheduler. We will use PyTorch's AdamW optimizer and linear learning rate scheduler since these are typically used to fine-tune BERT on tasks such as SST-2.

# COMMAND ----------

from torch.optim import AdamW
from torch.optim.lr_scheduler import LinearLR

optimizer = AdamW(
    params=composer_model.parameters(),
    lr=3e-5, betas=(0.9, 0.98),
    eps=1e-6, weight_decay=3e-6
)
linear_lr_decay = LinearLR(
    optimizer, start_factor=1.0,
    end_factor=0, total_iters=150
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Composer Trainer

# COMMAND ----------

# MAGIC %md
# MAGIC We will now specify a Composer `Trainer` object and run our training! `Trainer` has many arguments that are described in our [documentation](https://docs.mosaicml.com/en/stable/api_reference/generated/composer.Trainer.html#trainer), so we'll discuss only the less-obvious arguments used below:
# MAGIC
# MAGIC - `max_duration` - a string specifying how long to train. This can be in terms of batches (e.g., `'10ba'` is 10 batches) or epochs (e.g., `'1ep'` is 1 epoch), [among other options][time].
# MAGIC - `schedulers` - a (list of) PyTorch or Composer learning rate scheduler(s) that will be composed together.
# MAGIC - `device` - specifies if the training will be done on CPU or GPU by using `'cpu'` or `'gpu'`, respectively. You can omit this to automatically train on GPUs if they're available and fall back to the CPU if not.
# MAGIC - `train_subset_num_batches` - specifies the number of training batches to use for each epoch. This is not a necessary argument but is useful for quickly testing code.
# MAGIC - `precision` - whether to do the training in full precision (`'fp32'`) or mixed precision (`'amp'`). Mixed precision can provide a ~2x training speedup on recent NVIDIA GPUs.
# MAGIC - `seed` - sets the random seed for the training run, so the results are reproducible!
# MAGIC
# MAGIC [time]: https://docs.mosaicml.com/en/stable/trainer/time.html

# COMMAND ----------

# MAGIC %md
# MAGIC ### IMPORTANT NOTE
# MAGIC
# MAGIC A full pre-training run inside this notebook would take a VERY long time to complete. We will not be waiting for this job to run to completion. 
# MAGIC
# MAGIC After you launch the code cell below, look for the status messages and progress bar to see that the training work is starting. We will let the pre-training run progress for a few minutes, and then we will be stopping the run early.
# MAGIC
# MAGIC ### **Later in this session:** We will demo launching a pre-training run on MosaicML Cloud

# COMMAND ----------

import torch
from composer import Trainer

# Create Trainer Object
trainer = Trainer(
    model=composer_model, # This is the model from the HuggingFaceModel wrapper class.
    train_dataloader=train_dataloader,
    eval_dataloader=eval_dataloader,
    max_duration="1ep",
    optimizers=optimizer,
    schedulers=[linear_lr_decay],
    device='gpu' if torch.cuda.is_available() else 'cpu',
    train_subset_num_batches=150,
    eval_subset_num_batches=150,
    precision='fp32',
    seed=17
)
# Start training
trainer.fit()

# COMMAND ----------

# MAGIC %md
# MAGIC # **STOP HERE**
# MAGIC
# MAGIC This is the end of Module 2 - Pre-Training. We will take a break here, and make time for Q&A. However, if you want to continue without a break, feel free to do so.
# MAGIC
# MAGIC ## **Interrupt the previous cell!** 
# MAGIC **Before continuing to the next module: If the pre-training run in the cell above has not completed, please interrupt it.** To do that, click the "stop" square at the top-left corner of the notebook cell.
# MAGIC
# MAGIC ## **Module 3: Fine-Tuning**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Hugging Face Pretrained Model
# MAGIC First, we import a pretrained BERT model (specifically, BERT-base for uncased text) and its associated tokenizer from the transformers library.
# MAGIC
# MAGIC Sentiment classification has two labels, so we set `num_labels=2` when creating our model.

# COMMAND ----------

# Create a BERT sequence classification model using Hugging Face transformers
sentiment_model = transformers.AutoModelForSequenceClassification.from_pretrained('bert-base-uncased', num_labels=2)
sst2_tokenizer = transformers.AutoTokenizer.from_pretrained('bert-base-uncased') 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Dataloaders for Fine-Tuning
# MAGIC
# MAGIC When it's time to fine-tune a model, you don't need nearly as much data, and not nearly as much computational work, to get it done. A pre-trained BERT model has learned a great deal about the structure and context of the language. Because of the pre-training, fine-tuning for a specific use case can be done much more quickly.
# MAGIC
# MAGIC Next, we will download and tokenize the Stanford Sentiment Treebank v2 (SST-2) dataset. The SST-2 dataset contains a variety of sentences that have been labeled as having either Positive or Negative sentiment.

# COMMAND ----------

import datasets
from multiprocessing import cpu_count

# Create BERT tokenizer
def tokenize_function(sample):
    return sst2_tokenizer(
        text=sample['sentence'],
        padding="max_length",
        max_length=256,
        truncation=True
    )

# Tokenize SST-2
sst2_dataset = datasets.load_dataset("glue", "sst2")
tokenized_sst2_dataset = sst2_dataset.map(tokenize_function,
                                          batched=True, 
                                          num_proc=cpu_count(),
                                          batch_size=100,
                                          remove_columns=['idx', 'sentence'])

# Split dataset into train and validation sets
sst2_train_dataset = tokenized_sst2_dataset["train"]
sst2_eval_dataset = tokenized_sst2_dataset["validation"]

# COMMAND ----------

# MAGIC %md
# MAGIC Here, we will create a PyTorch `DataLoader` for each of the datasets generated in the previous block.

# COMMAND ----------

from torch.utils.data import DataLoader
sst2_data_collator = transformers.data.data_collator.default_data_collator
sst2_train_dataloader = DataLoader(sst2_train_dataset, batch_size=16, shuffle=False, drop_last=False, collate_fn=sst2_data_collator)
sst2_eval_dataloader = DataLoader(sst2_eval_dataset,batch_size=16, shuffle=False, drop_last=False, collate_fn=sst2_data_collator)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Composer Sentiment Analysis Model

# COMMAND ----------

from torchmetrics import Accuracy
from torchmetrics.collections import MetricCollection
from composer.metrics import CrossEntropy
from composer.models.huggingface import HuggingFaceModel

metrics = [CrossEntropy(), Accuracy()]
# Package as a trainer-friendly Composer model
composer_sentiment_model = HuggingFaceModel(sentiment_model, metrics=metrics, use_logits=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimizers and Learning Rate Schedulers
# MAGIC
# MAGIC The last setup step is to create an optimizer and a learning rate scheduler. We will use PyTorch's AdamW optimizer and linear learning rate scheduler since these are typically used to fine-tune BERT on tasks such as SST-2.

# COMMAND ----------

from torch.optim import AdamW
from torch.optim.lr_scheduler import LinearLR

sst2_optimizer = AdamW(
    params=composer_sentiment_model.parameters(),
    lr=3e-5, betas=(0.9, 0.98),
    eps=1e-6, weight_decay=3e-6
)
sst2_linear_lr_decay = LinearLR(
    sst2_optimizer, start_factor=1.0,
    end_factor=0, total_iters=150
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Composer Trainer
# MAGIC
# MAGIC Here is our second Composer `Trainer` object for the fine-tuning step. You can refer back to the `Trainer` [documentation](https://docs.mosaicml.com/en/stable/api_reference/generated/composer.Trainer.html#trainer), and review details from our previous `Trainer` in Module 2:
# MAGIC
# MAGIC - `max_duration` - a string specifying how long to train. This can be in terms of batches (e.g., `'10ba'` is 10 batches) or epochs (e.g., `'1ep'` is 1 epoch), [among other options][time].
# MAGIC - `schedulers` - a (list of) PyTorch or Composer learning rate scheduler(s) that will be composed together.
# MAGIC - `device` - specifies if the training will be done on CPU or GPU by using `'cpu'` or `'gpu'`, respectively. You can omit this to automatically train on GPUs if they're available and fall back to the CPU if not.
# MAGIC - `train_subset_num_batches` - specifies the number of training batches to use for each epoch. This is not a necessary argument but is useful for quickly testing code.
# MAGIC - `precision` - whether to do the training in full precision (`'fp32'`) or mixed precision (`'amp'`). Mixed precision can provide a ~2x training speedup on recent NVIDIA GPUs.
# MAGIC - `seed` - sets the random seed for the training run, so the results are reproducible!
# MAGIC
# MAGIC [time]: https://docs.mosaicml.com/en/stable/trainer/time.html

# COMMAND ----------

import torch
from composer import Trainer

# Create Trainer Object
sentiment_trainer = Trainer(
    model=composer_sentiment_model, # This is the model from the HuggingFaceModel wrapper class.
    train_dataloader=sst2_train_dataloader,
    eval_dataloader=sst2_eval_dataloader,
    max_duration="1ep",
    optimizers=sst2_optimizer,
    schedulers=[sst2_linear_lr_decay],
    device='gpu' if torch.cuda.is_available() else 'cpu',
    train_subset_num_batches=150,
    eval_subset_num_batches=150,
    precision='fp32',
    seed=17
)
# Start training
sentiment_trainer.fit()

# COMMAND ----------

# MAGIC %md
# MAGIC Looking at the `eval` accuracy metric in the final output, we can see our model reaches ~86% accuracy with only 150 iterations of training! 
# MAGIC Let's visualize a few samples from the validation set to see how our model performs.

# COMMAND ----------

# MAGIC %md
# MAGIC We can make our own predictions with the model now. Input your own string and see the sentiment prediction.

# COMMAND ----------

# Feel free to play around with this and change this string to your own input!
INPUT_STRING = "Hello, my dog is cute"

# COMMAND ----------

input_val = tokenizer(INPUT_STRING, return_tensors="pt")

input_batch = {k: v.cuda() if torch.cuda.is_available() else v for k, v in input_val.items()}

with torch.no_grad():
    logits = composer_sentiment_model(input_batch).logits
    
prediction = logits.argmax().item()

print(f"Raw prediction: {prediction}")

label = ['negative', 'positive']

print(f"Sentiment: {label[prediction]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Pre-Trained Model

# COMMAND ----------

# MAGIC %md
# MAGIC Finally, to save the pre-trained model parameters we call the PyTorch `save` method and pass it the model's `state_dict`: 

# COMMAND ----------

torch.save(sentiment_trainer.state.model.state_dict(), 'model.pt')

# COMMAND ----------

# MAGIC %md
# MAGIC # **Congratulations! LAB COMPLETE**
# MAGIC
# MAGIC You've now seen how to use the Composer `Trainer` to pre-train a Hugging Face BERT, using the C4 dataset. Following that, you've fine-tuned a Hugging Face BERT model on the SST-2 dataset, and seen how the model predicts a sentence to have positive or negative sentiment.
# MAGIC
# MAGIC ## What next?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Come get involved with MosaicML!
# MAGIC
# MAGIC We'd love for you to get involved with the MosaicML community in any of these ways:
# MAGIC
# MAGIC ### [Star Composer on GitHub](https://github.com/mosaicml/composer)
# MAGIC
# MAGIC Help make others aware of our work by [starring Composer on GitHub](https://github.com/mosaicml/composer).
# MAGIC
# MAGIC ### [Join the MosaicML Slack](https://join.slack.com/t/mosaicml-community/shared_invite/zt-w0tiddn9-WGTlRpfjcO9J5jyrMub1dg)
# MAGIC
# MAGIC Head on over to the [MosaicML slack](https://join.slack.com/t/mosaicml-community/shared_invite/zt-w0tiddn9-WGTlRpfjcO9J5jyrMub1dg) to join other ML efficiency enthusiasts. Come for the paper discussions, stay for the memes!
# MAGIC
# MAGIC ### Contribute to Composer
# MAGIC
# MAGIC Is there a bug you noticed or a feature you'd like? File an [issue](https://github.com/mosaicml/composer/issues) or make a [pull request](https://github.com/mosaicml/composer/pulls)!
