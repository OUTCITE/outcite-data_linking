#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
import sys, os
import time
import json
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import streaming_bulk as bulk
from common import *
from pathlib import Path
import multiprocessing as mp
#------------------------------------------------------------------------------------------------------------------------------------------------- #TODO: STILL NEEDS TO BE TESTED!
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------
_index = sys.argv[1]; #'geocite' #'ssoar'
_workers = int(sys.argv[2]) if len(sys.argv)>2 else 1;

IN = None;
try:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs_custom.json');
except:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs.json');
_configs = json.load(IN);
IN.close();

_buffer = _configs['buffer_econbiz'];

_chunk_size      = _configs['chunk_size_econbiz'];
_request_timeout = _configs['requestimeout_econbiz'];

_check   = _configs['check_econbiz'];
_recheck = _configs['recheck_econbiz'];
_retest  = _configs['retest_econbiz']; # Recomputes the URL even if there is already one in the index, but this should be conditioned on _recheck anyways, so only for docs where has_.._url=False
_resolve = _configs['resolve_econbiz']; # Replaces the URL with the redirected URL if there should be redirection

_refobjs = _configs['refobjs'];

URL = re.compile(_configs['regex_url']); #r'(http|ftp|https):\/\/([\w_-]+(?:(?:\.[\w_-]+)+))(([\w.\-\/,@?^=%&:~+#]|([\.\-\/=] ))*[\w@?^=%&\/~+#])'
DOI = re.compile(_configs['regex_doi']); #r'((https?:\/\/)?(www\.)?doi.org\/)?10.\d{4,9}\/[-._;()\/:A-Z0-9]+'
#====================================================================================
_index_m    = 'econbiz'; # Not actually required for crossref as the id is already the doi
_from_field = 'econbiz_id';
_to_field   = 'econbiz_urls';
#====================================================================================

_temp_parallel_results = None;
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

def append_result(result):
    global _temp_parallel_results
    _temp_parallel_results.append(result);

def get_url_for(refobject,listindex,field,id_field,cur,USE_BUFFER): #TODO: If I want to store the resolved urls then I need to return the results and store them outside this function!
    url        = None;
    resolution = (None,None,None,);
    if id_field in refobject and refobject[id_field] and (_retest or not (_to_field[:-1] in refobject and refobject[_to_field[:-1]])):
        opa_id                = refobject[id_field];
        page                  = _client_m.search(index=_index_m, body={"query":{"term":{"id.keyword":opa_id}}} );
        doi                   = page['hits']['hits'][0]['_source']['doi']  if len(page['hits']['hits'])>0 and 'doi'  in page['hits']['hits'][0]['_source'] else None;
        urls                  = page['hits']['hits'][0]['_source']['urls'] if len(page['hits']['hits'])>0 and 'urls' in page['hits']['hits'][0]['_source'] else [];
        url                   = doi2url(doi,cur,USE_BUFFER) if doi else urls[0] if urls else 'https://www.econbiz.de/Record/'+opa_id;
        url                   = urls[0] if (not url) and urls else 'https://www.econbiz.de/Record/'+opa_id if not url else url;
        url                   = check(url,_resolve,cur,5) if url else None;
        resolution            = (doi,418,doi,) if not url else (doi,200,url,);
        refobject[field[:-1]] = url;
    return [[url] if url else [], refobject, resolution, listindex];

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
