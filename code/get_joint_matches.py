import sys
import sqlite3
import numpy as np
from scipy.sparse import csr_matrix as csr
from scipy.sparse.csgraph import connected_components as components

_indb  = sys.argv[1];
_outdb = sys.argv[2]; # The dedup features DB

_refobjs = [    'anystyle_references_from_cermine_fulltext',
                'anystyle_references_from_cermine_refstrings',
                'anystyle_references_from_grobid_fulltext',
                'anystyle_references_from_grobid_refstrings',   #                'anystyle_references_from_gold_fulltext',
                'cermine_references_from_cermine_refstrings',          #                'anystyle_references_from_gold_refstrings',
                'cermine_references_from_grobid_refstrings',    #                'cermine_references_from_gold_refstrings',
                'grobid_references_from_grobid_xml' ];

con = sqlite3.connect(_indb);
cur = con.cursor();

print("Getting rows...");
rows = [];
for refobj in _refobjs:
    cur.execute("SELECT linkID,toID_sowiport,toID_crossref,toID_dnb,toID_openalex FROM links_"+refobj);
    rows += [[el for el in row if el] for row in cur];
con.close();

print("Indexing IDs...");
id2index = dict();
index2id = [];
for row in rows:
    for el in row:
        if not el in id2index:
            id2index[el] = len(index2id);
            index2id.append(el);

print("Creating sparse matrix...");
pairs      = [(id2index[row[0]],id2index[el],) for row in rows for el in row[1:]];
rows, cols = zip(*pairs);
L = csr((np.ones(len(rows)),(rows,cols)),shape=(len(id2index),len(id2index)),dtype=bool);

print("Detecting components...");
n, labels = components(L, directed=False);

print("Creating label to index mapping...");
label2indices = [];
for i in range(len(labels)):
    if labels[i] >= len(label2indices):
        label2indices.append([]);
    label2indices[labels[i]].append(i);

print("Storing labels...");
con = sqlite3.connect(_outdb);
cur = con.cursor();
cur.executemany("UPDATE mentions SET goldID=? WHERE originalID=?",((l,index2id[i],) for l in range(len(label2indices)) for i in label2indices[l]));
con.commit();
con.close();
