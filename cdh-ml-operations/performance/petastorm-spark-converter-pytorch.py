# Databricks notebook source
# MAGIC %md # Simplify data conversion from Spark to PyTorch
# MAGIC
# MAGIC This notebook demonstrates the following workflow on Databricks:
# MAGIC 1. Load data using Spark.
# MAGIC 2. Convert the Spark DataFrame to a PyTorch DataLoader using petastorm `spark_dataset_converter`.
# MAGIC 3. Feed the data into a single-node PyTorch model for training.
# MAGIC 4. Feed the data into a distributed hyperparameter tuning function.
# MAGIC 5. Feed the data into a distributed PyTorch model for training.
# MAGIC
# MAGIC The example in this notebook is based on the [transfer learning tutorial from PyTorch](https://pytorch.org/tutorials/beginner/transfer_learning_tutorial.html). It applies the pre-trained [MobileNetV2](https://pytorch.org/docs/stable/torchvision/models.html#mobilenet-v2) model to the flowers dataset.
# MAGIC
# MAGIC ### Requirements
# MAGIC 1. Databricks Runtime 7.3 LTS ML or above. On Databricks Runtime 6.x ML, you need to install petastorm==0.9.0 and pyarrow==0.15.0 on the cluster.
# MAGIC 2. Node type: one driver and two workers. Databricks recommends using GPU instances.

# COMMAND ----------

from pyspark.sql.functions import col

from petastorm.spark import SparkDatasetConverter, make_spark_converter

import io
import numpy as np
import torch
import torchvision
from PIL import Image
from functools import partial 
from petastorm import TransformSpec
from torchvision import transforms

from hyperopt import fmin, tpe, hp, SparkTrials, STATUS_OK

import horovod.torch as hvd
from sparkdl import HorovodRunner

# COMMAND ----------

BATCH_SIZE = 32
NUM_EPOCHS = 5

# COMMAND ----------

# MAGIC %md ## 1. Load data using Spark
# MAGIC
# MAGIC ### The flowers dataset
# MAGIC
# MAGIC This example uses the [flowers dataset](https://www.tensorflow.org/datasets/catalog/tf_flowers) from the TensorFlow team,
# MAGIC which contains flower photos stored under five subdirectories, one per class. The dataset is available under Databricks Datasets at `dbfs:/databricks-datasets/flower_photos`.
# MAGIC
# MAGIC The example loads the flowers table, which contains the preprocessed flowers dataset, using the binary file data source. To reduce running time, this notebook uses a small subset of the flowers dataset, including ~90 training images and ~10 validation images. When you run this notebook, you can increase the number of images used for better model accuracy.

# COMMAND ----------

df = spark.read.format("delta").load("/databricks-datasets/flowers/delta") \
  .select(col("content"), col("label"))
  
labels = df.select(col("label")).distinct().collect()
label_to_idx = {label: index for index, (label, ) in enumerate(sorted(labels))}
num_classes = len(label_to_idx)
df_train, df_val = df.limit(100).randomSplit([0.9, 0.1], seed=12345)

# Make sure the number of partitions is at least the number of workers which is required for distributed training.
df_train = df_train.repartition(2)
df_val = df_val.repartition(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Cache the Spark DataFrame using Petastorm Spark converter

# COMMAND ----------

# Set a cache directory on DBFS FUSE for intermediate data.
#spark.conf.set(SparkDatasetConverter.PARENT_CACHE_DIR_URL_CONF, "file:///dbfs/tmp/petastorm/cache")
#cachedir= "abfss://cdh@davsynapseanalyticsdev.dfs.core.windows.net/machinelearning"
dbutils.fs.ls(cachedir)
#spark.conf.set(SparkDatasetConverter.PARENT_CACHE_DIR_URL_CONF,cachedir)

converter_train = make_spark_converter(df_train)
converter_val = make_spark_converter(df_val)

# COMMAND ----------

print(f"train: {len(converter_train)}, val: {len(converter_val)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Feed the data into a single-node PyTorch model for training
# MAGIC
# MAGIC ### Get the model MobileNetV2 from torchvision

# COMMAND ----------

# First, load the model and inspect the structure of the model.
torchvision.models.mobilenet_v2(pretrained=True)

# COMMAND ----------

def get_model(lr=0.001):
  # Load a MobileNetV2 model from torchvision
  model = torchvision.models.mobilenet_v2(pretrained=True)
  # Freeze parameters in the feature extraction layers
  for param in model.parameters():
    param.requires_grad = False
    
  # Add a new classifier layer for transfer learning
  num_ftrs = model.classifier[1].in_features
  # Parameters of newly constructed modules have requires_grad=True by default
  model.classifier[1] = torch.nn.Linear(num_ftrs, num_classes)
  
  return model

# COMMAND ----------

# MAGIC %md ### Define the train and evaluate function for the model

# COMMAND ----------

def train_one_epoch(model, criterion, optimizer, scheduler, 
                    train_dataloader_iter, steps_per_epoch, epoch, 
                    device):
  model.train()  # Set model to training mode

  # statistics
  running_loss = 0.0
  running_corrects = 0

  # Iterate over the data for one epoch.
  for step in range(steps_per_epoch):
    pd_batch = next(train_dataloader_iter)
    inputs, labels = pd_batch['features'].to(device), pd_batch['label_index'].to(device)
    
    # Track history in training
    with torch.set_grad_enabled(True):
      # zero the parameter gradients
      optimizer.zero_grad()

      # forward
      outputs = model(inputs)
      _, preds = torch.max(outputs, 1)
      loss = criterion(outputs, labels)

      # backward + optimize
      loss.backward()
      optimizer.step()

    # statistics
    running_loss += loss.item() * inputs.size(0)
    running_corrects += torch.sum(preds == labels.data)
  
  scheduler.step()

  epoch_loss = running_loss / (steps_per_epoch * BATCH_SIZE)
  epoch_acc = running_corrects.double() / (steps_per_epoch * BATCH_SIZE)

  print('Train Loss: {:.4f} Acc: {:.4f}'.format(epoch_loss, epoch_acc))
  return epoch_loss, epoch_acc

def evaluate(model, criterion, val_dataloader_iter, validation_steps, device, 
             metric_agg_fn=None):
  model.eval()  # Set model to evaluate mode

  # statistics
  running_loss = 0.0
  running_corrects = 0

  # Iterate over all the validation data.
  for step in range(validation_steps):
    pd_batch = next(val_dataloader_iter)
    inputs, labels = pd_batch['features'].to(device), pd_batch['label_index'].to(device)

    # Do not track history in evaluation to save memory
    with torch.set_grad_enabled(False):
      # forward
      outputs = model(inputs)
      _, preds = torch.max(outputs, 1)
      loss = criterion(outputs, labels)

    # statistics
    running_loss += loss.item()
    running_corrects += torch.sum(preds == labels.data)
  
  # Average the losses across observations for each minibatch.
  epoch_loss = running_loss / validation_steps
  epoch_acc = running_corrects.double() / (validation_steps * BATCH_SIZE)
  
  # metric_agg_fn is used in the distributed training to aggregate the metrics on all workers
  if metric_agg_fn is not None:
    epoch_loss = metric_agg_fn(epoch_loss, 'avg_loss')
    epoch_acc = metric_agg_fn(epoch_acc, 'avg_acc')

  print('Validation Loss: {:.4f} Acc: {:.4f}'.format(epoch_loss, epoch_acc))
  return epoch_loss, epoch_acc

# COMMAND ----------

# MAGIC %md ### Preprocess images
# MAGIC
# MAGIC Before feeding the dataset into the model, you need to decode the raw image bytes and apply standard ImageNet transforms. Databricks recommends not doing this transformation on the Spark DataFrame since that substantially increases the size of the intermediate files and might decrease performance. Instead, do this transformation in a `TransformSpec` function in petastorm.

# COMMAND ----------

def transform_row(is_train, pd_batch):
  """
  The input and output of this function must be pandas dataframes.
  Do data augmentation for the training dataset only.
  """
  transformers = [transforms.Lambda(lambda x: Image.open(io.BytesIO(x)))]
  if is_train:
    transformers.extend([
      transforms.RandomResizedCrop(224),
      transforms.RandomHorizontalFlip(),
    ])
  else:
    transformers.extend([
      transforms.Resize(256),
      transforms.CenterCrop(224),
    ])
  transformers.extend([
    transforms.ToTensor(),
    transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225]),
  ])
  
  trans = transforms.Compose(transformers)
  
  pd_batch['features'] = pd_batch['content'].map(lambda x: trans(x).numpy())
  pd_batch['label_index'] = pd_batch['label'].map(lambda x: label_to_idx[x])
  pd_batch = pd_batch.drop(labels=['content', 'label'], axis=1)
  return pd_batch

def get_transform_spec(is_train=True):
  # The output shape of the `TransformSpec` is not automatically known by petastorm, 
  # so you need to specify the shape for new columns in `edit_fields` and specify the order of 
  # the output columns in `selected_fields`.
  return TransformSpec(partial(transform_row, is_train), 
                       edit_fields=[('features', np.float32, (3, 224, 224), False), ('label_index', np.int32, (), False)], 
                       selected_fields=['features', 'label_index'])

# COMMAND ----------

# MAGIC %md ### Train and evaluate the model on the local machine
# MAGIC
# MAGIC Use `converter.make_torch_dataloader(...)` to create the dataloader.

# COMMAND ----------

def train_and_evaluate(lr=0.001):
  device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
  
  model = get_model(lr=lr)
  model = model.to(device)

  criterion = torch.nn.CrossEntropyLoss()

  # Only parameters of final layer are being optimized.
  optimizer = torch.optim.SGD(model.classifier[1].parameters(), lr=lr, momentum=0.9)

  # Decay LR by a factor of 0.1 every 7 epochs
  exp_lr_scheduler = torch.optim.lr_scheduler.StepLR(optimizer, step_size=7, gamma=0.1)
  
  with converter_train.make_torch_dataloader(transform_spec=get_transform_spec(is_train=True), 
                                             batch_size=BATCH_SIZE) as train_dataloader, \
       converter_val.make_torch_dataloader(transform_spec=get_transform_spec(is_train=False), 
                                           batch_size=BATCH_SIZE) as val_dataloader:
    
    train_dataloader_iter = iter(train_dataloader)
    steps_per_epoch = len(converter_train) // BATCH_SIZE
    
    val_dataloader_iter = iter(val_dataloader)
    validation_steps = max(1, len(converter_val) // BATCH_SIZE)
    
    for epoch in range(NUM_EPOCHS):
      print('Epoch {}/{}'.format(epoch + 1, NUM_EPOCHS))
      print('-' * 10)

      train_loss, train_acc = train_one_epoch(model, criterion, optimizer, exp_lr_scheduler, 
                                              train_dataloader_iter, steps_per_epoch, epoch, 
                                              device)
      val_loss, val_acc = evaluate(model, criterion, val_dataloader_iter, validation_steps, device)

  return val_loss
  
loss = train_and_evaluate()

# COMMAND ----------

# MAGIC %md ## 4. Feed the data into a distributed hyperparameter tuning function.
# MAGIC
# MAGIC Use Hyperopt SparkTrials for hyperparameter tuning.
# MAGIC The converter is picklable and will be used to generate a PyTorch DataLoader on the driver node and remote worker nodes.

# COMMAND ----------

def train_fn(lr):
  loss = train_and_evaluate(lr)
  return {'loss': loss, 'status': STATUS_OK}

search_space = hp.loguniform('lr', -10, -4)

argmin = fmin(
  fn=train_fn,
  space=search_space,
  algo=tpe.suggest,
  max_evals=2,
  trials=SparkTrials(parallelism=2))

# COMMAND ----------

# See optimized hyperparameters
argmin

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Feed the data into a distributed PyTorch model for training.
# MAGIC
# MAGIC Use HorovodRunner for distributed training.
# MAGIC
# MAGIC The example uses the default value of parameter `num_epochs=None` to generate infinite batches of data to avoid handling the last incomplete batch. This is particularly useful in the distributed training scenario, where you need to guarantee that the numbers of data records seen on all workers are identical per step. Given that the length of each data shard may not be identical, setting `num_epochs` to any specific number would fail to meet the guarantee.

# COMMAND ----------

def metric_average(val, name):
  tensor = torch.tensor(val)
  avg_tensor = hvd.allreduce(tensor, name=name)
  return avg_tensor.item()

def train_and_evaluate_hvd(lr=0.001):
  hvd.init()  # Initialize Horovod.
  
  # Horovod: pin GPU to local rank.
  if torch.cuda.is_available():
    torch.cuda.set_device(hvd.local_rank())
    device = torch.cuda.current_device()
  else:
    device = torch.device("cpu")
  
  model = get_model(lr=lr)
  model = model.to(device)

  criterion = torch.nn.CrossEntropyLoss()
  
  # Effective batch size in synchronous distributed training is scaled by the number of workers.
  # An increase in learning rate compensates for the increased batch size.
  optimizer = torch.optim.SGD(model.classifier[1].parameters(), lr=lr * hvd.size(), momentum=0.9)
  
  # Broadcast initial parameters so all workers start with the same parameters.
  hvd.broadcast_parameters(model.state_dict(), root_rank=0)
  hvd.broadcast_optimizer_state(optimizer, root_rank=0)
  
  # Wrap the optimizer with Horovod's DistributedOptimizer.
  optimizer_hvd = hvd.DistributedOptimizer(optimizer, named_parameters=model.named_parameters())

  exp_lr_scheduler = torch.optim.lr_scheduler.StepLR(optimizer_hvd, step_size=7, gamma=0.1)

  with converter_train.make_torch_dataloader(transform_spec=get_transform_spec(is_train=True), 
                                             cur_shard=hvd.rank(), shard_count=hvd.size(),
                                             batch_size=BATCH_SIZE) as train_dataloader, \
       converter_val.make_torch_dataloader(transform_spec=get_transform_spec(is_train=False),
                                           cur_shard=hvd.rank(), shard_count=hvd.size(),
                                           batch_size=BATCH_SIZE) as val_dataloader:
    
    train_dataloader_iter = iter(train_dataloader)
    steps_per_epoch = len(converter_train) // (BATCH_SIZE * hvd.size())
    
    val_dataloader_iter = iter(val_dataloader)
    validation_steps = max(1, len(converter_val) // (BATCH_SIZE * hvd.size()))
    
    for epoch in range(NUM_EPOCHS):
      print('Epoch {}/{}'.format(epoch + 1, NUM_EPOCHS))
      print('-' * 10)

      train_loss, train_acc = train_one_epoch(model, criterion, optimizer_hvd, exp_lr_scheduler, 
                                              train_dataloader_iter, steps_per_epoch, epoch, 
                                              device)
      val_loss, val_acc = evaluate(model, criterion, val_dataloader_iter, validation_steps,
                                   device, metric_agg_fn=metric_average)

  return val_loss

# COMMAND ----------

hr = HorovodRunner(np=2)   # This assumes the cluster consists of two workers.
hr.run(train_and_evaluate_hvd)
