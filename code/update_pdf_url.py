#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys, os
import time
import json
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
from common import *
from pathlib import Path
import sqlite3
import re
import multiprocessing as mp
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------
_index   = sys.argv[1]; #'geocite' #'ssoar'
_dbfile  = sys.argv[2];#'resources/doi2pdfs.db';
_target  = sys.argv[3] if len(sys.argv)>3 else None;
_workers = int(sys.argv[4]) if len(sys.argv)>4 else 1;

_mapping = _dbfile.split('/')[-1].split('.')[0];

IN = None;
try:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs_custom.json');
except:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs.json');
_configs = json.load(IN);
IN.close();

_buffer = _configs['buffer_pdf'];

_chunk_size      = _configs['chunk_size_pdf'];
_request_timeout = _configs['requestimeout_pdf'];

_check   = _configs['check_pdf'];
_recheck = _configs['recheck_pdf'];
_retest  = _configs['retest_pdf']; # Recomputes the URL even if there is already one in the index, but this should be conditioned on _recheck anyways, so only for docs where has_.._url=False
_resolve = _configs['resolve_pdf']; # Replaces the URL with the redirected URL if there should be redirection

_refobjs = _configs['refobjs']; #TODO: I think this is not used?

ARXIVURL = re.compile(_configs['regex_arxiv_url']); #"((https?:\/\/www\.)|(https?:\/\/)|(www\.))arxiv\.org\/(abs|pdf)\/[0-9]+\.[0-9]+(\.pdf)?"
ARXIVPDF = re.compile(_configs['regex_arxiv_pdf']); #"((https?:\/\/www\.)|(https?:\/\/)|(www\.))arxiv\.org\/pdf\/[0-9]+\.[0-9]+(\.pdf)?"
ARXIVID  = re.compile(_configs['regex_arxiv_id']);  #"[0-9]+\.[0-9]+"

URL = re.compile('regex_url'); #r'(http|ftp|https):\/\/([\w_-]+(?:(?:\.[\w_-]+)+))(([\w.\-\/,@?^=%&:~+#]|([\.\-\/=] ))*[\w@?^=%&\/~+#])'
DOI = re.compile('regex_doi'); #r'((https?:\/\/)?(www\.)?doi.org\/)?10.\d{4,9}\/[-._;()\/:A-Z0-9]+'
#====================================================================================
_from_field = _target+'_id' if _target=='ssoar' or _target=='arxiv' else _target+'_doi' if _target else 'doi';
_to_field   = _target+'_'+_mapping+'_fulltext_urls' if _target else 'extracted_'+_mapping+'_fulltext_urls'; # WARNING: The difference to the usual procedure is that this is used multiple times for different _target, which means processed_fulltext_url=true
#====================================================================================

_temp_parallel_results = None;
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

def append_result(result):
    global _temp_parallel_results
    _temp_parallel_results.append(result);

def get_best_pdf_url(urls): #TODO: Can be specified
    return urls[0] if len(urls)>0 else None;

def get_url_for(refobject,listindex,field,id_field,cur,USE_BUFFER): #TODO: If I want to store the resolved urls then I need to return the results and store them outside this function!
    con_lookup = sqlite3.connect(_dbfile);
    cur_lookup = con_lookup.cursor();
    con_buffer = sqlite3.connect('urls_'+_index+'.db') if (not cur) and USE_BUFFER else None;
    cur_buffer = con_buffer.cursor() if (not cur) and USE_BUFFER else cur;
    urls_etc   = [];
    urls_pdf   = [];
    ids        = [];
    resolution = (None,None,None,);
    if id_field=='ssoar_id' and 'ssoar_id' in refobject and refobject['ssoar_id'] and (_retest or not (_to_field[:-1] in refobject and refobject[_to_field[:-1]])):
        handle     = refobject['ssoar_id'].split('-')[-1];
        url        = 'https://www.ssoar.info/ssoar/bitstream/handle/document/'+handle+'/?sequence=1';
        url_       = check(url,_resolve,cur_buffer,5,USE_BUFFER) if _check else url if url and URL.match(url) else None;
        resolution = (None,None,None,) if not _check else (url,418,url,) if not url_ else (url,200,url_,); #TODO: Is this correct?
        if url_:
            urls_pdf.append(url_);
    elif id_field=='arxiv_id' and 'arxiv_id' in refobject and refobject['arxiv_id'] and (_retest or not (_to_field[:-1] in refobject and refobject[_to_field[:-1]])):
        url        = 'https://arxiv.org/pdf/'+refobject['arxiv_id']+'.pdf';
        url_       = url;# Don't strain arxiv.org too much... #check(url,_resolve,cur_buffer,5,USE_BUFFER) if _check else url if url and URL.match(url) else None;
        resolution = (url,200,url,);#(None,None,None,) if not _check else (url,418,url,) if not url_ else (url,200,url_,); #TODO: Is this correct?
        if url_:
            urls_pdf.append(url_);
    elif id_field in refobject and refobject[id_field] and (_retest or not (_to_field[:-1] in refobject and refobject[_to_field[:-1]])):
        doi        = refobject[id_field].lower().rstrip('.'); print('--->',doi);
        arxiv      = extract_arxiv_id(doi);
        url_       = doi2url(doi,cur_buffer,USE_BUFFER) if not (doi.startswith('arxiv:') or (doi.startswith('abs/') and ARXIVID.search(doi))) else 'https://arxiv.org/pdf/'+arxiv+'.pdf' if arxiv else None;
        alt        = [url_] if url_ and url_.endswith('.pdf') else [];
        urls_      = [row[0] for row in cur_lookup.execute("SELECT pdf_url FROM doi2pdfs WHERE doi=? ORDER BY id DESC",(doi,)).fetchall()]+alt;
        resolution = (doi,418,doi,) if not url_ else (doi,200,url_,);
        for url in urls_:
            if url and not ARXIVURL.match(url):
                url = url if isinstance(url,str) else None;#check(url,_resolve,cur_buffer,5,USE_BUFFER) if url else None;
            else:
                print(url);
            if url:
                if url.endswith('.pdf'):
                    print('Adding',url,'to PDF urls...');
                    urls_pdf.append(url);
                else:
                    print('Adding',url,'to general urls...');
                    urls_etc.append(url);
    else:
        return [ids, refobject, resolution, listindex];
    urls = list(set(urls_pdf + urls_etc));
    for url in urls:
        ID = url;#check(url,_resolve,5) if url else None;
        if ID != None:
            #refobject[field[:-1]] = ID;
            ids.append(ID);
        #break;
    pdfurls                    = [url for url in urls if url.endswith('.pdf') or url.endswith('/?sequence=1') or ARXIVPDF.match(url)];
    refobject[field]           = urls;
    refobject['fulltext_urls'] = list(set(refobject['fulltext_urls']+pdfurls)) if 'fulltext_urls' in refobject else pdfurls; #only previous fulltext urls or .pdf urls
    refobject['fulltext_url']  = get_best_pdf_url(refobject['fulltext_urls']);
    con_lookup.close();
    if (not cur) and USE_BUFFER:
        con_buffer.close();
    return [ids, refobject, resolution, listindex];

def get_url(refobjects,field,id_field,cur=None,USE_BUFFER=None): # This actually gets the doi not the url 
    global _temp_parallel_results
    _temp_parallel_results = [];
    ids                    = [];
    resolutions            = [];
    pool                   = mp.Pool(_workers) if _workers>1 else None;
    for i in range(len(refobjects)):
        if _workers>1:
            pool.apply_async(get_url_for, args=(refobjects[i],i,field,id_field,None,'r' if (USE_BUFFER=='rw' or USE_BUFFER=='r') else None,), callback=append_result);
        else:
            _temp_parallel_results.append(get_url_for(refobjects[i],i,field,id_field,cur,USE_BUFFER));
    if _workers>1:
        pool.close(); pool.join();
    #print(_temp_parallel_results);
    for ids_,refobject,resolution,listindex in _temp_parallel_results:
        ids                  += ids_;
        refobjects[listindex] = refobject; #TODO: BUG: i was out of scope! Does it work now?
        resolutions.append(resolution);
    if _workers > 1 and cur and (USE_BUFFER=='rw' or USE_BUFFER=='w'): # Write the results to DB after the parallel step, uses 418 for error and 200 for success
        print('Storing resolutions to DB...')
        cur.executemany("INSERT OR REPLACE INTO urls VALUES(?,?,?)",((url,status,new_url,) for url,status,new_url in resolutions if url and status and new_url));
    return set(ids), refobjects;

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

_client   = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);
_client_m = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);

i = 0;
for success, info in bulk(_client,search(_to_field,_from_field,_index,_recheck,get_url,_buffer),chunk_size=_chunk_size, request_timeout=_request_timeout):
    i += 1;
    if not success:
        print('\n[!]-----> A document failed:', info['index']['_id'], info['index']['error'],'\n');
    print(i,info)
    if i % _chunk_size == 0:
        print(i,'refreshing...');
        _client.indices.refresh(index=_index);
print(i,'refreshing...');
_client.indices.refresh(index=_index);
#-------------------------------------------------------------------------------------------------------------------------------------------------
