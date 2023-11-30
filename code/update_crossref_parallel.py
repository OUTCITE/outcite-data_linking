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

# THE INDEX TO UPDATE THE REFERENCES IN
_index = sys.argv[1];
# THE NUMBER OF PARALLEL PROCESSES FOR RESOLVING URLS
_workers = int(sys.argv[2]) if len(sys.argv)>2 else 1;

# LOADING THE CONFIGS CUSTOM IF AVAILABLE OTHERWISE THE DEFAULT CONFIGS FILE
IN = None;
try:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs_custom.json');
except:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs.json');
_configs = json.load(IN);
IN.close();

# PARAMETERS FOR THE BULK UPDATING ELASTICSEARCH PROCESS
_chunk_size      = _configs['chunk_size_crossref'];
_request_timeout = _configs['requestimeout_crossref'];

# WHETHER TO BUFFER THE RESULTING URLS IN A LOCAL DATABASE
_buffer = _configs['buffer_crossref'];

# WHETHER TO CHECK IF THE URL GOES ANYWHERE
_check   = _configs['check_crossref'];
# WETHER TO REDO THE LINKING FOR DOCUMENTS THAT HAVE ALREADY BEEN LABELLED AS PROCESSED FOR THIS STEP BEFORE
_recheck = _configs['recheck_crossref'];
# WHETHER TO TEST THE URL EVEN IF IT WAS ALREADY SEEN BEFORE
_retest  = _configs['retest_crossref']; # Recomputes the URL even if there is already one in the index, but this should be conditioned on _recheck anyways, so only for docs where has_.._url=False
# WHETHER TO REPLACE A URL BY THE END OF A REDIRECT CHAIN
_resolve = _configs['resolve_crossref']; # Replaces the URL with the redirected URL if there should be redirection

# REGEX FOR URLS AND DOIS
URL = re.compile(_configs['regex_url']); #r'(http|ftp|https):\/\/([\w_-]+(?:(?:\.[\w_-]+)+))(([\w.\-\/,@?^=%&:~+#]|([\.\-\/=] ))*[\w@?^=%&\/~+#])'
DOI = re.compile(_configs['regex_doi']); #r'((https?:\/\/)?(www\.)?doi.org\/)?10.\d{4,9}\/[-._;()\/:A-Z0-9]+'
#====================================================================================
# NAME OF THE ELASTICSEARCH INDEX TO MATCH AGAINST
_index_m    = 'crossref';
# FIELD NAME IN THE TARGET INDEX WHICH STORES THE ID FROM MATCHING
_from_field = 'crossref_id';
# WHERE TO ADD THE URL FROM THE ABOVE TARGET INDEX FIELD
_to_field   = 'crossref_urls';
#====================================================================================

# TECHNICALLY REQUIRED TO STORE RESULTS FROM PARALLELIZED PROCESSING
_temp_parallel_results = None;
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

# TECHNICALLY REQUIRED TO STORE RESULTS FROM PARALLELIZED PROCESSING
def append_result(result):
    global _temp_parallel_results
    _temp_parallel_results.append(result);

# OUTSOURCED URL RESOLUTION FOR PARALLEL PROCESSING
def get_url_for(refobject,listindex,field,id_field,cur,USE_BUFFER): #TODO: If I want to store the resolved urls then I need to return the results and store them outside this function!
    url        = None;
    resolution = (None,None,None,);
    if id_field in refobject and refobject[id_field] and (_retest or not (_to_field[:-1] in refobject and refobject[_to_field[:-1]])):
        doi                   = doi2url(refobject[id_field],cur,USE_BUFFER,_resolve);
        url                   = check(doi,_resolve,cur,5) if doi and _check else doi if doi else None;
        resolution            = (doi,418,doi,) if not url else (doi,200,url,);
        refobject[field[:-1]] = url;
    return [[url] if url else [], refobject, resolution, listindex];

# MAIN FUNCTION TO GET THE URL FOR A REFERENCE IF IT HAS A MATCH ID
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

# CONNECTION TO THE LOCAL ELASTICSEARCH INSTANCE WHERE THE INDEX IS
_client = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);

# BATCH UPDATING THE LOCAL DOCUMENTS INDEX WITH THE URLS
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
