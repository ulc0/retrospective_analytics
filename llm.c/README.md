# llm.c

LLM training in simple, pure C/CUDA. There is no need for 245MB of PyTorch or 107MB of cPython. For example, training GPT-2 (CPU, fp32) is ~1,000 lines of clean code in a single file. It compiles and runs instantly, and exactly matches the PyTorch reference implementation. I chose GPT-2 as the first working example because it is the grand-daddy of LLMs, the first time the modern stack was put together.

Currently, I am working on:

- direct CUDA implementation, which will be significantly faster and probably come close to PyTorch.
- speed up the CPU version with SIMD instructions, AVX2 on x86 / NEON on ARM (e.g. Apple Silicon).
- more modern architectures, e.g. Llama2, Gemma, etc.

For the repo, I'd like to maintain both clean, simple reference implementations alongside a also lot more optimized versions that can come close to PyTorch, but in a tiny fraction of the code and dependencies.

## quick start

Download and tokenize a dataset. The [tinyshakespeare](https://raw.githubusercontent.com/karpathy/char-rnn/master/data/tinyshakespeare/input.txt) dataset is the fastest to download and tokenize:

```bash
python prepro_tinyshakespeare.py
```

This prints:

```
Saved 32768 tokens to data/tiny_shakespeare_val.bin
Saved 305260 tokens to data/tiny_shakespeare_train.bin
```

The .bin files are raw byte streams of int32 numbers indicating the token ids with the GPT-2 tokenizer. Alternatively you could also tokenize the [TinyStories](https://huggingface.co/datasets/roneneldan/TinyStories) dataset with `prepro_tinystories.py`.

In principle we'd be ready to the train the model right here. However the baseline CPU/fp32 reference code is so inefficient that it's not practical to train these models from scratch yet. Instead, we initialize with the GPT-2 weights released by OpenAI and just do finetuning. For that, we have to download the GPT-2 weights and save them as a checkpoint we can load in C:

```bash
python train_gpt2.py
```

You'll recognize this code from nanoGPT as a simple GPT-2 reference implementation in PyTorch. This script will download the GPT-2 (124M) model, overfit a single batch of data for 10 iterations, run a few steps of generation, and most importantly it will save two files: 1) the `gpt2_124M.bin` file that contains the raw model weights for loading in C, and `gpt2_124M_debug_state.bin`, which also contains more debug state: the inputs, targets, logits and loss. This is very useful for debugging C code, for unit testing, and making sure we're exactly matching the PyTorch reference implementation. For now all we care about are the model weights in `gpt2_124M.bin`. We can now initialize with them and train in raw C. First compile the code:

```bash
make train_gpt2
```

You can have a look inside the `Makefile` and its comments. It will try to autodetect if OpenMP is available on your system, which is very helpful for speeding up the code at very low cost of code complexity. Once `train_gpt2` is compiled, you can run it:

```bash
OMP_NUM_THREADS=8 ./train_gpt2
```

You should tune the number of threads depending on how many cores your CPU has. The program will load the model weights, the tokens, it will run a finetuning loop for a few iterations with Adam lr 1e-4, and then generate a sample from the model. The file is (I think) very readable and you should have a look. Simply, there are implementations for the forward and backward pass of all the layers, and they get strung together into a large, manual, forward/backward/update loop. The output looks like this on my MacBook Pro (Apple Silicon M3 Max):

```
[GPT-2]
max_seq_len: 1024
vocab_size: 50257
num_layers: 12
num_heads: 12
channels: 768
num_parameters: 124439808
train dataset num_batches: 1192
val dataset num_batches: 128
num_activations: 73323776
val loss 5.252026
step 0: train loss 5.356189 (took 1452.121000 ms)
step 1: train loss 4.301069 (took 1288.673000 ms)
step 2: train loss 4.623322 (took 1369.394000 ms)
step 3: train loss 4.600470 (took 1290.761000 ms)
... (trunctated) ...
step 39: train loss 3.970751 (took 1323.779000 ms)
val loss 4.107781
generated: 50256 16773 18162 21986 11 198 13681 263 23875 198 3152 262 11773 2910 198 1169 6002 6386 2583 286 262 11858 198 20424 428 3135 7596 995 3675 13 198 40 481 407 736 17903 11 329 703 6029 706 4082 198 42826 1028 1128 633 263 11 198 10594 407 198 2704 454 680 1028 262 1027 28860 286 198 3237 323
step 40: train loss 4.377757 (took 1366.368000 ms)
```

The generation just gives you the token ids for now, which we have to decode back to text. We can implement this in C quite easily also, because decoding is very straight-forward, it's just string chunk lookups and prints. For now we can use tiktoken:

```python
import tiktoken
enc = tiktoken.get_encoding("gpt2")
print(enc.decode(list(map(int, "50256 16773 18162 21986 11 198 13681 263 23875 198 3152 262 11773 2910 198 1169 6002 6386 2583 286 262 11858 198 20424 428 3135 7596 995 3675 13 198 40 481 407 736 17903 11 329 703 6029 706 4082 198 42826 1028 1128 633 263 11 198 10594 407 198 2704 454 680 1028 262 1027 28860 286 198 3237 323".split()))))
```

which prints:

```
<|endoftext|>Come Running Away,
Greater conquer
With the Imperial blood
the heaviest host of the gods
into this wondrous world beyond.
I will not back thee, for how sweet after birth
Netflix against repounder,
will not
flourish against the earlocks of
Allay
```

I like how Netflix comes up, it's clear that the shadow of the training past is still lurking in the model. I did not attempt to tune the finetuning hyperparameters so it's quite likely this can be improved quite a bit, most likely especially if one was to train a bit longer.

## test

I am also attaching a simple unit test for making sure our C code agrees with the PyTorch code. Compile and run with:

```
make test_gpt2
./test_gpt2
```

This now loads the `gpt2_124M_debug_state.bin` file, runs a forward pass, compares the logits and loss with the PyTorch reference implementation, then it does 10 iterations of training with Adam and makes sure the losses match PyTorch.

## tutorial

I attached a very small tutorial here, in [doc/layernorm/layernorm.md](doc/layernorm/layernorm.md). It's a simple, step-by-step guide to implementing a single layer of the GPT-2 model, the layernorm layer. This is a good starting point to understand how the layers are implemented in C.

## license

MIT