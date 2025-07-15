import argparse
import itertools
import numpy as np
import operator
import os
import pickle
import spacy
import scispacy
import time

from scipy.sparse import load_npz
from scispacy.umls_utils import UmlsKnowledgeBase


def get_unresolved_tuples(sent_text, nlp, kb, st):
    """ Returns tuples representing entity annotations from SciSpacy's UMLS
        interface. Candidate entities identified by SciSpacy are untyped, i.e.,
        their entity.label_ is ENTITY. These entities are then handed off to
        the UmlsKnowledgeBase which links each entity span to zero or more UMLS
        concepts and associated semantic type codes.

        Parameters
        ----------
            sent_text (str): the sentence text
            nlp -- the SciSpacy language model
            kb -- the SciSpacy UMLS Knowledge Base
            st -- the Semantic Type Tree for UMLS

        Returns
        -------
            list of annotations with following elements:
                ent_id: a running serial number starting at 0,
                entity_span: the entity text span,
                cid: the UMLS concept id,
                pname: the primary name of the UMLS concept,
                stycode: the semantic code for the concept,
                styname: the semantic type name for the concept
    """
    doc = nlp(sent_text)
    # entity_spans = [ent.text for ent in doc.ents]
    entity_spans = []
    for ent in doc.ents:
        # print(ent.label_, ent.text)
        entity_spans.append(ent.text)

    # print(entity_spans)
    annotations = []
    for ent_id, entity_span in enumerate(entity_spans):
        try:
            cuis = kb.alias_to_cuis[entity_span]
            for cui in cuis:
                umls_ent = kb.cui_to_entity[cui]
                cid = umls_ent.concept_id
                pname = umls_ent.canonical_name
                stycodes = [(stycode, st.get_canonical_name(stycode)) 
                            for stycode in umls_ent.types]
                for stycode, styname in stycodes:
                    annotations.append((ent_id, entity_span, 
                                        cid, pname, 
                                        stycode, styname))
        except KeyError:
            continue
    return annotations


def print_tuples(tuples, title):
    """ Pretty prints the tuples

        Parameters
        ----------
            tuples: list of annotation tuples (see #get_unresolved_tuples)
            title (str): title for the tuple list.

        Returns
        -------
            None
    """
    prev_entid = None
    print("--- {:s} ---".format(title))
    for t in tuples:
        ent_id, ent_text, con_id, con_name, sty_id, sty_name = t
        if prev_entid is not None and prev_entid == ent_id:
            cont_char = "+"
        else:
            cont_char = " "
        print("{:s}{:d} | {:s} | {:s} | {:s} | {:s} | {:s}".format(
            cont_char, ent_id, ent_text, con_id, con_name, sty_id, sty_name))
        prev_entid = ent_id


def load_serialized_matrix(data_dir, prefix):
    """ Load serialized matrix and lookup tables (see 07a-build-matrices-gist.py)
        The matrix is a two dimensional sparse CSR matrix and is accompanied by
        lookup tables for its x and y axes. Lookup tables associate the business
        object (the concept ID and semantic code) to the x and y index into the 
        matrix.

        NOTE: the Transition matrix specifies transition probabilities between
        adjacent semantic types, and the Emission matrix specifies transition
        probabilities of a concept given a semantic type.

        Parameters
        ----------
            data_dir (str): location of the data folder where the matrix 
                artifacts are located
            prefix (str): the matrix prefix. The matrix builder code stores 
                the matrix as ${prefix}_matrix.npz, and the lookup tables as
                ${prefix}_xindex.pkl and ${prefix}_yindex.pkl respectively.

        Returns
        -------
            the sparse matrix
            lookup table for x
            lookup table for y
    """
    matrix_file = os.path.join(data_dir, "{:s}_matrix.npz".format(prefix))
    with open(matrix_file, "rb") as f:
        M = load_npz(f)
    xindex_file = os.path.join(data_dir, "{:s}_xindex.pkl".format(prefix))
    with open(xindex_file, "rb") as f:
        xindex = pickle.load(f)
    yindex_file = os.path.join(data_dir, "{:s}_yindex.pkl".format(prefix))
    with open(yindex_file, "rb") as f:
        yindex = pickle.load(f)
    return M, xindex, yindex


def get_stycode_sequence(annotations):
    """ Produces a graph representing the sentence for which annotation list
        is provided. Nodes in the graph are the semantic type codes reported
        by SciSpacy + UMLS integration.

        Parameters
        ----------
            annotations: list[(ent_id, ent_span, con_id, con_pname,
                sty_id, sty_name)]

        Returns
        -------
            the graph of semantic type codes for the sentence.
    """
    ent2sty = {}
    for annotation in annotations:
        ent_id, _, con_id, _, sty_id, _ = annotation
        if ent_id in ent2sty.keys():
            ent2sty[ent_id].add(sty_id)
        else:
            ent2sty[ent_id] = set([sty_id])
    stys_seq = [["B"]]
    for ent_id in sorted(list(ent2sty.keys())):
        stys_seq.append(list(ent2sty[ent_id]))
    return stys_seq


def viterbi_forward(stys_seq, T, T_x, T_y):
    """ Implements the forward part of the Viterbi algorithm. At each
        point, we compute the transition probability as the product of 
        the maximum probability for the source node and the transition 
        probability from the source to the destination node.

        Parameters
        ----------
            stys_seq (list(list(str))): each element in the outer list 
                corresponds to a position in the semantic code sequence,
                and each position in the inner list corresponds to a 
                possible semantic code for the entity at that position, 
                as reported by the SciSpacy+UMLS integration.
            T (scipy.sparse.csr_matrix): a CSR sparse matrix representing the 
                probabilities to transition between consecutive semantic 
                type codes in the annotations.
            T_x (dict(str, int)): dictionary of semantic code to row index.
            T_y (dict(str, int)): dictionary of semantci code to column index.

        Returns
        --------
            graph of node probabilities for each semantic code in the graph
                produced by #get_stycode_sequence
    """
    node_probs_seq = [[("B", 0.0)]]
    prev_node2prob = None
    # compute edge probabilities by decomposing into bigram positions
    for i in range(len(stys_seq) - 1):
        # at each successive stycode position, find all possible edges
        edges = itertools.product(*stys_seq[i : i + 2])
        curr_node2prob, node_probs = {}, []
        for t1, t2 in edges:
            # print("edge:", t1, t2)
            # compute prob to reach node t2
            trans_prob = T[T_x[t1], T_y[t2]]
            if trans_prob == 0:
                trans_prob = 1e-19
            # compute emission prob of concept(s) C2 given
            # NOOP here since we don't have ambiguity going from 
            # sty code to concept
            logprob = np.log(trans_prob)
            if prev_node2prob is not None:
                # add the cumulative log prob as well
                logprob += prev_node2prob[t1] 
            # store only max prob for each t2
            if t2 not in curr_node2prob.keys():
                curr_node2prob[t2] = logprob
            else:
                if curr_node2prob[t2] < logprob:
                    curr_node2prob[t2] = logprob
        node_probs_seq.append([(k, v) for k, v in curr_node2prob.items()])
        prev_node2prob = curr_node2prob
    return node_probs_seq


def viterbi_backward(probs_seq):
    """ Implements the backward part of the Viterbi algorithm. It navigates 
        the node probability graph backwards to find the most likely sequence.

        Parameters
        ----------
            probs_seq (list(list(float))): graph of probabilities for each node
                in the graph of semantic type codes.

        Returns
        -------
            the most likely sequence of semantic type codes as a list(str)
    """
    best_seq = []
    for node_probs in probs_seq[::-1]:
        # print("node_probs:", node_probs)
        best_seq.append(sorted(node_probs, key=operator.itemgetter(1), reverse=True)[0])
    return [x[0] for x in best_seq[::-1] if x[0] != "B"]


def resolve_tuples(unresolved_tuples, best_sequence, E, E_x, E_y):
    """ Uses the most likely sequence of semantic type codes to prune 
        list of unresolved tuples. Since a semantic type can resolve to
        multiple concepts, we use the emission probabilities (probability
        of a concept given a semantic type) to choose the most popular 
        concept among the alternatives for the given semantic type.

        Parameters
        ----------
            unresolved_tuples (list(annotations)): annotations tuples are
                described in #get_unresolved_tuples.
            E (csr_matrix(float)): the emission matrix of concept_id given 
                a particular semantic type.
            E_x (dict(sty_code, row)): lookup table to convert from semantic
                type code to row index.
            E_y (dict(concept_id, col)): lookup table to convert from concept
                ID to column index.

        Returns
        -------
            a list(annotation) containing a list of most likely disambiguated 
                annotations for the sentence.
    """
    ent2tuple, pos2ent, pos = {}, {}, 0
    for t in unresolved_tuples:
        ent_id, ent_text, con_id, con_name, sty_id, sty_name = t
        if ent_id not in ent2tuple.keys():
            ent2tuple[ent_id] = [t]
            pos2ent[pos] = ent_id
            pos += 1 
        else:
            ent2tuple[ent_id].append(t)
    resolved_tuples = []
    for pos, pred_sty_id in enumerate(best_sequence):
        ent_id = pos2ent[pos]
        candidate_tuples = [t for t in ent2tuple[ent_id] if t[4] == pred_sty_id]
        if len(candidate_tuples) > 1:
            # if multiple concepts mapped per entity, choose the more
            # popular concept for that sty code
            emission_probs = []
            for candidate_tuple in candidate_tuples:
                cand_con_id, cand_sty_id = candidate_tuple[2], candidate_tuple[4]
                try:
                    e_x = E_x[cand_sty_id]
                    e_y = E_y[cand_con_id]
                    eprob = E[E_x[cand_sty_id], E_y[cand_con_id]]
                    if eprob == 0:
                        eprob = 1e-14
                except KeyError:
                    eprob = 1e-14
                emission_probs.append((candidate_tuple, np.log(eprob)))
            candidate_tuples = [x[0] for x in 
                sorted(emission_probs, key=operator.itemgetter(1), reverse=True)]
        resolved_tuples.append(candidate_tuples[0])
    return resolved_tuples


def do_entity_disambiguation(sent_text, nlp, kb, st):
    """ Harness to run entity disambiguation for a single sentence.

        Parameters
        -----------
            sent_text (str): the sentence to annotate and disambiguate with UMLS.
            nlp: the SciSpacy language model to identify entities.
            kb: the UMLS Knowledge base (part of SciSpacy + UMLS integration) to
                link entity spans to UMLS concepts and associated sty codes.
            st: the UMLS Semantic Type Tree, used to lookup one or more semantic
                type names for the semantic code.

        Returns
        -------
            None
    """

    start = time.time()

    print("sentence:", sent_text)

    unresolved_tuples = get_unresolved_tuples(sent_text, nlp, kb, st)
    print_tuples(unresolved_tuples, "Entities linked by SciSpacy (before Disambiguation)")

    T, T_x, T_y = load_serialized_matrix("../data", "T")
    E, E_x, E_y = load_serialized_matrix("../data", "E")
    print(T.shape, len(T_x), len(T_y))
    print(E.shape, len(E_x), len(E_y))

    stys_seq = get_stycode_sequence(unresolved_tuples)
    print("Graph:", stys_seq)

    probs_seq = viterbi_forward(stys_seq, T, T_x, T_y)
    print("Cumulative Probabilities:", probs_seq)

    best_seq = viterbi_backward(probs_seq)
    print("Best Sequence:", best_seq)

    resolved_tuples = resolve_tuples(unresolved_tuples, best_seq, E, E_x, E_y)
    print_tuples(resolved_tuples, "After Entity Disambiguation")

    print("elapsed: {:.3f}".format(time.time() - start))


if __name__ == "__main__":
    sentences = [
        """PA using two agonists (adenosine diphosphate; ADP and arachidonic acid; ASPI) and a saline control was performed in six healthy client‐owned dogs at baseline and 1 week following clopidogrel administration to establish cut‐off values to identify responders (R) or non‐responders (NR)."""
    ]

    print("Loading SciSpacy Language Model...")
    nlp = spacy.load("en_core_sci_md", disable=["tagger", "parser"])

    print("Loading UMLS Knowledge Base...")
    kb = UmlsKnowledgeBase()
    st = kb.semantic_type_tree

    for sentence in sentences:
        print("---")
        do_entity_disambiguation(sentence, nlp, kb, st)
