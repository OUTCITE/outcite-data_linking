import sqlite3
import json
import os,sys
import multiprocessing as MP
import time
import requests
import re
from collections import Counter
import itertools
import operator
from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords
from nltk.corpus import wordnet as WN
from nltk.stem.wordnet import WordNetLemmatizer
from unidecode import unidecode as UD
import asciidammit as dammit
import re
from symspellpy import SymSpell, Verbosity
import pkg_resources
from cld3 import get_language as detect


_indb  = sys.argv[1];
_outdb = sys.argv[2];

_simple_freqs = 1.0;

_dump      = 10000;
_wait      = 0.001;

_maxyear = 2023;

_dict_edit_dist = 4;
_ratio          = 0.2;

# The three cases used are <words,parts> / <word_ngrams,parts> / <char_ngrams,char_ngrams>
_termfeats       = 'char_grams_by_word' #'word_ngrams'; #'words' 'char_grams' 'char_grams_by_word'
_authfeats       = 'parts' #'parts' 'char_grams'
_wordsep_authors = True;
_n_authors       = 5;
_n_terms         = 5;

_symspells = dict();
for code,filename in [('default',"frequency_dictionary_en_82_765.txt",),('de',"de-100k.txt",),('fr',"fr-100k.txt",),('es',"es-100l.txt",),('ru',"ru-100k.txt",),('it',"it-100k.txt",)]:
    _symspells[code] = SymSpell(max_dictionary_edit_distance=_dict_edit_dist, prefix_length=7);
    _symspells[code].load_dictionary(pkg_resources.resource_filename("symspellpy", filename), term_index=0, count_index=1);

_stopwords = set().union(*[set(stopwords.words(lang)) for lang in ['english','german','french','italian','spanish','russian','portuguese','dutch','swedish','danish','finnish']]);
_tokenizer = RegexpTokenizer(r'\w+')
_surpres   = set(['de','del','di','de la','von','van','della']);

NONAME    = re.compile(r'(.*anonym\w*)|(.*unknown\w*)|(\s*-\s*)');
WORD      = re.compile(r'(\b[^\s]+\b)'); #TODO: Make stricter
STRIP     = re.compile(r'(^(\s|,)+)|((\s|,)+$)');
PUNCT     = re.compile(r'[!"#$%&\'()*+\/:;<=>?@[\\\]^_`{|}~1-9]'); #Meaningless punctuation for Author name lists, excludes , . -
SUBTITDIV = re.compile(r'\. |: | -+ |\? ');
STOPWORDS = re.compile(r'&|\.|\,|'+r'|'.join(['\\b'+stopword+'\\b' for stopword in _stopwords]));

WNL = WordNetLemmatizer();

def concat(object1, object2):
    if isinstance(object1, str):
        object1 = [object1]
    if isinstance(object2, str):
        object2 = [object2]
    return object1 + object2

def capitalize(word):
    return word[0].upper() + word[1:]

def is_word(string,code):
    return len(string) > 2 and (string in _stopwords or len(WN.synsets(string)) > 0 or len(_symspells[code].lookup(string, Verbosity.CLOSEST,max_edit_distance=0, include_unknown=False))>0);

def splitter(string, language):
    for index, char in enumerate(string):
        left_compound         = string[0:-index];
        right_compound_1      = string[-index:];
        right_compound_2      = string[-index+1:];
        right_compound1_upper = right_compound_1[0].isupper() if right_compound_1 else None;
        right_compound2_upper = right_compound_2[0].isupper() if right_compound_2 else None;
        left_compound         = capitalize(left_compound) if index > 0 and len(left_compound) > 1 and not is_word(left_compound,language) else left_compound;
        left_compound_valid   = is_word(left_compound,language);
        #print(left_compound,right_compound_1,right_compound_2,right_compound1_upper,right_compound2_upper,left_compound,left_compound_valid);
        if left_compound_valid and ((not splitter(right_compound_1,language) == '' and not right_compound1_upper) or right_compound_1 == ''):
            return [compound for compound in concat(left_compound, splitter(right_compound_1, language)) if not compound == ''];
        if left_compound_valid and string[-index:-index+1] == 's' and ((not splitter(right_compound_2, language) == '' and not right_compound2_upper) or right_compound_2 == ''):
            return [compound for compound in concat(left_compound, splitter(right_compound_2, language)) if not compound == ''];
    return [string] if not string == '' and is_word(string,language) else [capitalize(string)] if not string == '' and is_word(capitalize(string),language) else '';

def split(string, language): # Only called for strings where not is_word
    if string in _stopwords or len(string) <= 2:
        return [string.lower()];
    parts = splitter(string,language);
    return [string.lower()] if len(parts)==0 else [part.lower() for part in parts];

def correct(string,code):
    suggestions = [suggestion.term for suggestion in _symspells[code].lookup(string, Verbosity.CLOSEST,max_edit_distance=min(_dict_edit_dist,int(len(string)*_ratio)), include_unknown=False)];
    string_     = suggestions[0] if len(suggestions) > 0 else string;
    return string_;

def get_language(text):
    language = detect(text);
    language = language.language if language else language;
    language = 'default' if not language in set(['de','fr','ru','uk','es','pt','it']) else language;
    language = 'ru' if language=='uk' else language;
    language = 'es' if language=='pt' else language;
    #print(language,':',text);
    return language;

def get_char_ngrams(title,n=4,wordsep=False):
    if title == None:
        return [];
    title  = dammit.asciiDammit(title.lower().replace(' ','_'));
    words  = title.split('_') if wordsep else [title];
    ngrams = [];
    for word in words:
        ngrams += [word[i:i+n] for i in range(len(word)-(n-1))];
    return ngrams;

def get_words(text):
    if text == None:
        return [];
    language = get_language(text);
    text     = text.lower();
    words    = [dammit.asciiDammit(word) for word in _tokenizer.tokenize(text) if not word in _stopwords];
    words    = [correct(word,language) if not is_word(word,language) else word for word in words];
    return words;

def get_word_ngrams(text):
    if text == None:
        return [];
    language = get_language(text);
    known_bi = [];
    unknown  = [];
    text     = text.lower();
    texts    = SUBTITDIV.split(text);
    sections = [division for text_ in texts for division in STOPWORDS.split(text_)];
    for section in sections:
        words = [dammit.asciiDammit(word) for word in _tokenizer.tokenize(section) if not word in _stopwords];
        words = [correct(word,language) if not is_word(word,language) else word for word in words];
        known = [];
        for word in words:
            if is_word(word,language):
                known.append(word);
            else:
                subwords = split(word,language);
                if len(subwords) > 1:
                    known += subwords;
                else:
                    unknown += subwords;
        for i in range(len(known)):
            posses   = [(sum([lemma.count() for lemma in synset.lemmas()]),synset.pos()) for synset in WN.synsets(known[i])]
            pos      = posses[-1][1] if len(posses) > 0 else None
            known[i] = WNL.lemmatize(known[i],pos) if pos != None else WNL.lemmatize(known[i]);#lemmas[0][1];
        bigrams   = [known[i]+' '+known[i+1] for i in range(len(known)-1)] if len(known) > 1 else known;
        known_bi += bigrams;
    terms  = known_bi + unknown;
    check  = set([]);
    terms_ = [];
    for term in terms:
        if not term in check:
            terms_.append(term);
            check.add(term);
    return terms_;

def authgrams(a1surname,a1init,a1first,a2surname,a2init,a2first,a3surname,a3init,a3first,a4surname,a4init,a4first):
    a1s                       = '_'.join((el for el in [a1surname,a1init,a1first] if el));
    a2s                       = '_'.join((el for el in [a2surname,a2init,a2first] if el));
    a3s                       = '_'.join((el for el in [a3surname,a3init,a3first] if el));
    a4s                       = '_'.join((el for el in [a4surname,a4init,a4first] if el));
    a1grams                   = get_char_ngrams(a1s,_n_authors,_wordsep_authors);
    a2grams                   = get_char_ngrams(a2s,_n_authors,_wordsep_authors);
    a3grams                   = get_char_ngrams(a3s,_n_authors,_wordsep_authors);
    a4grams                   = get_char_ngrams(a4s,_n_authors,_wordsep_authors);
    agrams                    = a1grams + a2grams + a3grams + a4grams;
    return agrams[:12] if len(agrams) >= 12 else agrams+[None for x in range(12-len(agrams))]; # This means that longer names will overwrite later ones

def get_years(year):
    return [int(str(year-1)+str(year)), int(str(year)+str(year+1))] if isinstance(year,int) and year <= _maxyear else [None,None];

def get_features(cur):
    row_num      = 0;
    index2linkID = [];
    linkID2index = dict();
    num_rows     = cur.execute("SELECT COUNT(*) FROM refmetas").fetchall()[0][0];
    times        = {'years':0,'source':0,'title':0,'authors':0};
    cur.execute("SELECT * FROM refmetas");
    for linkID, fromPipeline, sowiportID, crossrefID, dnbID, openalexID, issue, volume, year, source, title, a1sur, a1init, a1first, a2sur, a2init, a2first, a3sur, a3init, a3first, a4sur, a4init, a4first, e1sur, e1init, e1first, publisher1 in cur:
        row_num += 1;
        if row_num % 1000 == 0:
            print(round(100.0*row_num/num_rows,2),'%  ',times,end='\r');
        if not linkID in linkID2index:
            linkID2index[linkID] = len(index2linkID);
            index2linkID.append(linkID);
        t = time.time();
        year1, year2                                                                                   = get_years(year); times['years'] += time.time()-t; t = time.time();
        source_term1, source_term2, source_term3, source_term4, source_term5, source_term6             = (get_words(source)+[None for i in range(6)])[:6]; times['source'] += time.time()-t; t = time.time();
        title_term1, title_term2, title_term3, title_term4, title_term5, title_term6                   = (get_words(title) +[None for i in range(6)])[:6]; times['title'] += time.time()-t; t = time.time();
        a1sur, a1init, a1first, a2sur, a2init, a2first, a3sur, a3init, a3first, a4sur, a4init, a4first = [dammit.asciiDammit(part.lower()) if part else None for part in [a1sur, a1init, a1first, a2sur, a2init, a2first, a3sur, a3init, a3first, a4sur, a4init, a4first]]; times['authors'] += time.time()-t;
        #print('--------------------------------------------------------');
        #print(year1, year2)
        #print('|'.join([el for el in [source_term1, source_term2, source_term3, source_term4, source_term5, source_term6]             if el]))
        #print('|'.join([el for el in [title_term1, title_term2, title_term3, title_term4, title_term5, title_term6]                   if el]))
        #print('|'.join([el for el in [a1sur, a1init, a1first, a2sur, a2init, a2first, a3sur, a3init, a3first, a4sur, a4init, a4first] if el]))
        #print('--------------------------------------------------------');
        yield (linkID2index[linkID],linkID,None,1.0,sowiportID,crossrefID,dnbID,openalexID,title,year1,year2,a1sur,a1init,a1first,a2sur,a2init,a2first,a3sur,a3init,a3first,a4sur,a4init,a4first,title_term1, title_term2, title_term3, title_term4, title_term5, title_term6,source_term1, source_term2, source_term3, source_term4, source_term5, source_term6,);

#-----------------------------------------------------------------------------------------------------------------------
#  --> Lower case and ASCII
#  --> Split source and title into phrases
#  --> Make year like 20002001

con     = sqlite3.connect(_indb);
cur     = con.cursor();
con_out = sqlite3.connect(_outdb);
cur_out = con_out.cursor();

cur_out.execute("DROP TABLE IF EXISTS mentions");
cur_out.execute('CREATE TABLE mentions(mentionID INT, originalID TEXT, goldID TEXT, freq REAL, sowiportID TEXT, crossrefID TEXT, dnbID TEXT, openalexID TEXT, title TEXT, year1 INT, year2 INT, a1sur TEXT, a1init TEXT, a1first TEXT, a2sur TEXT, a2init TEXT, a2first TEXT, a3sur TEXT, a3init TEXT, a3first TEXT, a4sur TEXT, a4init TEXT, a4first TEXT, term1 TEXT, term2 TEXT, term3 TEXT, term4 TEXT, term5 TEXT, term6 TEXT, term1gen TEXT, term2gen TEXT, term3gen TEXT, term4gen TEXT, term5gen TEXT, term6gen TEXT)');

cur_out.executemany("INSERT INTO mentions VALUES("+','.join(['?' for i in range(35)])+")",get_features(cur));
con_out.commit();

cur_out.execute("CREATE INDEX IF NOT EXISTS originalID_index ON mentions(originalID)");

con.close();
con_out.close();

# linkID, fromPipeline
# issue, volume, year
# source_term1, source_term2, source_term3, source_term4, source_term5, source_term6
# title_term1, title_term2, title_term3, title_term4, title_term5, title_term6
# a1sur, a1init, a1first, a2sur, a2init, a2first, a3sur, a3init, a3first, a4sur, a4init, a4first
# e1sur, e1init, e1first
# publisher1
#-----------------------------------------------------------------------------------------------------------------------
