import streamlit as st
import spacy
from spacy import displacy
import pandas as pd

from scispacy.umls_linking import UmlsEntityLinker
from scispacy.abbreviation import AbbreviationDetector


SPACY_MODEL_NAMES = ["en_core_sci_sm", "en_core_sci_md", "en_core_sci_lg"]
NER_MODEL_NAMES = ["en_ner_craft_md", "en_ner_jnlpba_md", "en_ner_bc5cdr_md", "en_ner_bionlp13cg_md"]
DEFAULT_TEXT = "Spinal and bulbar muscular atrophy (SBMA) is an inherited motor neuron disease caused by the expansion of a polyglutamine tract within the androgen receptor (AR). SBMA can be caused by this easily."
HTML_WRAPPER = """<div style="overflow-x: auto; border: 1px solid #e6e9ef; border-radius: 0.25rem; padding: 1rem; margin-bottom: 2.5rem">{}</div>"""


@st.cache(allow_output_mutation=True)
def load_model(name):

    nlp = spacy.load(name)
    # Add abbreviation detector
    abbreviation_pipe = AbbreviationDetector(nlp)
    nlp.add_pipe(abbreviation_pipe)
    return nlp

@st.cache(allow_output_mutation=True)
def process_text(model_name, text):
    nlp = load_model(model_name)
    return nlp(text)

@st.cache(allow_output_mutation=True)
def load_linker():
    linker = UmlsEntityLinker(resolve_abbreviations=True)

    return linker


st.title("Scispacy - Demo")


st.sidebar.markdown(
    """
Analyze text with [ScispaCy](https://allenai.github.io/scispacy/) models and visualize entity linking, named entities,
dependencies and more.
"""
)

spacy_model = st.sidebar.selectbox("Model name", SPACY_MODEL_NAMES)
model_load_state = st.info(f"Loading model '{spacy_model}'...")
nlp = load_model(spacy_model)
model_load_state.empty()

linker = load_linker()

st.sidebar.header("Entity Linking")
threshold = st.sidebar.slider("Mention Threshold", 0.0, 1.0, 0.85)
linker.threshold = threshold
show_only_top = st.sidebar.checkbox("Show only top entity per mention", value=True)

st.sidebar.header("Specialized NER")
ner_model = st.sidebar.selectbox("NER Model", NER_MODEL_NAMES)
ner_load_state = st.info(f"Loading NER Model '{ner_model}'...")
ner = load_model(ner_model)
ner_load_state.empty()


st.header("Enter some text:")
text = st.text_area("", DEFAULT_TEXT)
doc = process_text(spacy_model, text)
ner_doc = process_text(ner_model, text)



st.header("Entity Linking")
st.markdown("Mentions are detected with the standard pipeline's mention detector.")


html = displacy.render(doc, style="ent")
# Newlines seem to mess with the rendering
html = html.replace("\n", " ")
st.write(HTML_WRAPPER.format(html), unsafe_allow_html=True)


data = []
for ent in linker(doc).ents:
    for ent_id, score in ent._.umls_ents:

        kb_entity = linker.umls.cui_to_entity[ent_id]
        tuis = ",".join(kb_entity.types)
        data.append([
            ent.text,
            kb_entity.canonical_name,
            ent_id,
            tuis,
            score,
            ent.start,
            ent.end,
        ])

        if show_only_top:
            break

attrs = ["text", "Canonical Name", "Concept ID", "TUI(s)", "Score", "start", "end"]
df = pd.DataFrame(data, columns=attrs)
dfStyler = df.style.set_properties(**{'text-align': 'left'})
dfStyler.set_table_styles([dict(selector='th', props=[('text-align', 'left')])])

st.markdown("Entities are linked to the Unified Medical Language System (UMLS).")
st.table(dfStyler)

st.header("Specialized NER")
labels = st.sidebar.multiselect(
    "Entity labels",
    ner.get_pipe("ner").labels, # Options
    ner.get_pipe("ner").labels # Default to all selected.
)
html = displacy.render(ner_doc, style="ent", options={"ents": labels})
# Newlines seem to mess with the rendering
html = html.replace("\n", " ")
st.write(HTML_WRAPPER.format(html), unsafe_allow_html=True)

attrs = ["text", "label_", "start", "end", "start_char", "end_char"]
data = [
    [str(getattr(ent, attr)) for attr in attrs]
    for ent in ner_doc.ents
]
df = pd.DataFrame(data, columns=attrs)
st.dataframe(df)


st.header("Dependency Parse & Part-of-speech tags")
if st.button("Show Parser and Tagger"):
    st.sidebar.header("Dependency Parse")
    split_sents = st.sidebar.checkbox("Split sentences", value=True)
    collapse_punct = st.sidebar.checkbox("Collapse punctuation", value=True)
    collapse_phrases = st.sidebar.checkbox("Collapse phrases")
    compact = st.sidebar.checkbox("Compact mode")
    options = {
        "collapse_punct": collapse_punct,
        "collapse_phrases": collapse_phrases,
        "compact": compact,
    }
    docs = [span.as_doc() for span in doc.sents] if split_sents else [doc]
    for sent in docs:
        html = displacy.render(sent, options=options)
        # Double newlines seem to mess with the rendering
        html = html.replace("\n\n", "\n")
        if split_sents and len(docs) > 1:
            st.markdown(f"> {sent.text}")
        st.write(HTML_WRAPPER.format(html), unsafe_allow_html=True)

st.header("Token attributes")
if st.button("Show token attributes"):
    attrs = [
        "idx",
        "text",
        "lemma_",
        "pos_",
        "tag_",
        "dep_",
        "head",
        "ent_type_",
        "ent_iob_",
        "shape_",
        "is_alpha",
        "is_ascii",
        "is_digit",
        "is_punct",
        "like_num",
    ]
    data = [[str(getattr(token, attr)) for attr in attrs] for token in doc]
    df = pd.DataFrame(data, columns=attrs)
    st.dataframe(df)


st.header("JSON Doc")
if st.button("Show JSON Doc"):
    st.json(doc.to_json())

st.header("JSON model meta")
if st.button("Show JSON model meta"):
    st.json(nlp.meta)


st.sidebar.header("Acknowledgements")
st.sidebar.markdown(
    """
Adapted from [Ines Montani](https://ines.io/)'s brilliant [spaCy](https://spacy.io) Streamlit Demo to work with Scispacy.
Scispacy is maintained by [Mark Neumann](https://markneumann.xyz) and [Daniel King](https://www.linkedin.com/in/daniel-king-2190a595) at the [Allen Institute for Artificial Intelligence](https://allenai.org/).
"""
)
