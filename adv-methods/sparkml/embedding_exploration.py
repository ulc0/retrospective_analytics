# Databricks notebook source

dbutils.widgets.text('model_name' , defaultValue= "alvaroalon2/biobert_diseases_ner",label="Huggingface Hub Name")
architecture = dbutils.widgets.get("model_name")
print(architecture)

# COMMAND ----------

from transformers import AutoTokenizer, AutoModel
from sklearn.metrics.pairwise import cosine_similarity
import torch
import numpy as np

# Load the pretrained model and tokenizer
tokenizer = AutoTokenizer.from_pretrained(str(architecture))
model = AutoModel.from_pretrained(str(architecture))

def get_embeddings(words):
    """
    Generate embeddings for a list of words using a pretrained transformer model.

    Parameters:
    words (list of str): List of words for which to generate embeddings.

    Returns:
    list of numpy.ndarray: List of embeddings, one per input word.
    """
    # Tokenize the words and convert to input tensors
    tokens = tokenizer(words, padding=True, truncation=True, return_tensors='pt', is_split_into_words=False)
    
    # Extract input IDs and attention masks
    input_ids = tokens['input_ids']
    attention_mask = tokens['attention_mask']
    
    with torch.no_grad():
        # Get the model outputs
        outputs = model(input_ids=input_ids, attention_mask=attention_mask)
        hidden_states = outputs.last_hidden_state
    
    # Average embeddings across tokens for each word
    embeddings = []
    for i, word in enumerate(words):
        # Get the token embeddings for the current word
        word_tokens = tokenizer.convert_ids_to_tokens(input_ids[i])
        token_embeddings = hidden_states[i, :len(word_tokens)]
        
        # Average the embeddings for the current word
        word_embedding = torch.mean(token_embeddings, dim=0).numpy()
        embeddings.append(word_embedding)
    
    return embeddings

def find_closest_words(important_embeddings, vocabulary_embeddings, vocabulary_words, top_n=5):
    """
    Find the closest vocabulary words to each important word based on embeddings.

    Parameters:
    important_embeddings (list of numpy.ndarray): Embeddings of important words.
    vocabulary_embeddings (list of numpy.ndarray): Embeddings of vocabulary words.
    vocabulary_words (list of str): List of vocabulary words corresponding to the embeddings.
    top_n (int): Number of closest words to return.

    Returns:
    dict: Dictionary with important words as keys and lists of closest words as values.
    """
    closest_words = {}
    
    for i, imp_embedding in enumerate(important_embeddings):

        imp_word = important_words[i]
        # Compute cosine similarity between the important word and all vocabulary words
        similarities = cosine_similarity([imp_embedding], vocabulary_embeddings)[0]
        
        # Get indices of the top_n most similar words
        top_indices = np.argsort(similarities)[-top_n:]
        
        # Retrieve the closest words
        closest = [vocabulary_words[idx] for idx in reversed(top_indices)]
        closest_words[imp_word] = closest
        #closest_words[vocabulary_words[i]] = closest
    
    return closest_words

# Define a list of vocabulary words
important_words = ['hearing_loss', 'anxiety', 'abdominal_pain', 'hvid', 'stroke']
vocabulary_words = (
    spark.table("""edav_prd_cdh.cdh_abfm_phi_exploratory.mpox_xgboost_data""")
    .drop(*("label","person_id","the","aaa",'2','s'))
    .columns
)

# Generate embeddings for the vocabulary
important_embeddings = get_embeddings(important_words)
vocabulary_embeddings = get_embeddings(vocabulary_words)

closest_words = find_closest_words(important_embeddings, vocabulary_embeddings, vocabulary_words, 5)

# COMMAND ----------

type(vocabulary_words)

# COMMAND ----------

# Print the results
for imp_word, close_words in closest_words.items():
    print(f"Important Word: {imp_word}")
    print(f"Closest Words: {close_words}\n")

# COMMAND ----------


