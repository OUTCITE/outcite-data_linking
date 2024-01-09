#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
import requests
import time
import sys
import json
from pathlib import Path
import re
import sqlite3
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBAL OBJECTS----------------------------------------------------------------------------------------------------------------------------------

# LOADING THE CONFIGS CUSTOM IF AVAILABLE OTHERWISE THE DEFAULT CONFIGS FILE
IN = None;
try:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs_custom.json');
except:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs.json');
_configs = json.load(IN);
IN.close();

# MORE PARAMETERS FOR THE BULK UPDATING ELASTICSEARCH PROCESS
_max_extract_time = _configs['max_extract_time']; #minutes
_max_scroll_tries = _configs['max_scroll_tries'];
_scroll_size      = _configs['scroll_size'];

# THE PIPELINES FOR WHICH TO AMEND THE REFERENCES
_refobjs = _configs['refobjs'];

# THE SUBSET TO UPDATE IF ANY
_ids = _configs['ids'];

# REGEX FOR ARXIV IDS
ARXIVID = re.compile(_configs['regex_arxiv_id']); #r"[0-9]+\.[0-9]+"
#-------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS---------------------------------------------------------------------------------------------------------------------------------------

# CHECK AND POSSIBLY RESOLVE A GIVEN URL INCLUDING BUFFERING OF THE MAPPING TO LOCAL DATABASE
def check(url,RESOLVE=False,cur=None,timeout=5,USE_BUFFER=None):
    print('Checking URL',url,'...');
    page   = None;
    status = None;
    try:
        status = None;
        if cur and USE_BUFFER=='r' or USE_BUFFER=='rw': # r means that we only use buffer to read not write, rw means to read and write and w should mean only write to buffer
            print('Trying to read resolution for',url,'from DB...')
            rows    = cur.execute("SELECT status,resolve FROM urls WHERE url=?",(url,)).fetchall();
            status  = rows[0][0] if rows and rows[0] else None;
            new_url = rows[0][1] if rows and rows[0] else None;
            if status:
                print('Found existing resolution for',url,'in DB:',status,new_url);
        if not status:
            print('Trying to resolve',url,'by http request...')
            page    = requests.head(url,allow_redirects=True,timeout=timeout) if RESOLVE else requests.head(url,timeout=timeout);
            status  = page.status_code;
            new_url = page.url;
            print('Managed to resolve',url,'by http request:',status,new_url);
            if cur and USE_BUFFER=='rw' or USE_BUFFER=='w':
                print('Storing resolution',status,'-->',new_url,'for',url,'to DB...')
                cur.execute("INSERT OR REPLACE INTO urls VALUES(?,?,?)",(url,status,new_url,));
        if status in [400,404]+list(range(407,419))+list(range(500,511)):
            print('----> Could not resolve URL due to',status,url);
            return None;
    except Exception as e:
        print('ERROR:',e, file=sys.stderr);
        print('----> Could not resolve URL due to above exception',url);
        return None;
    if new_url:
        print('Successfully resolved URL',url,'to',new_url);
    else:
        print('----> Could not resolve URL for some reason',url,'-- status:',status);
    return new_url if RESOLVE else url;

# EXTRACT THE ARXIV ID OUT OF A GIVEN STRING
def extract_arxiv_id(string):
    ids = [match.group() for match in ARXIVID.finditer(string)];
    return ids[0] if ids else None;

# TURN A DOI INTO A URL AND POSSIBLY FOLLOW THE REDIRECT CHAIN
def doi2url(doi,cur=None,USE_BUFFER=None,RESOLVE=True):
    url   = 'https://doi.org/'+doi;
    return check(url,RESOLVE,cur,5,USE_BUFFER);

# MAIN FUNCTION TO UPDATE THE FIELD GIVEN THE ID FIELD
def search(field,id_field,index,recheck,get_url,USE_BUFFER=None): #TODO: That line 91 scr_query did not solve the problem yet
    #----------------------------------------------------------------------------------------------------------------------------------
    body      = { '_op_type': 'update', '_index': index, '_id': None, '_source': { 'doc': { 'processed_'+field: True, field: None } } };
    scr_query = { "ids": { "values": _ids } } if _ids else { 'bool':{'must_not':  {'term':{'processed_'+field: True}}, 'should': [{'term':{'has_'+id_field+'s':True}},{'term':{'has_'+id_field.split('_')[0]+'_references_by_matching':True}}] } } if not recheck else {'bool':{'should':[{'term':{'has_'+id_field+'s':True}},{'term':{'has_'+id_field.split('_')[0]+'_references_by_matching':True}}]}};
    if id_field=='doi':
        scr_query = { "ids": { "values": _ids } } if _ids else { 'bool':{'must_not':  {'term':{'processed_'+field: True}}}} if not recheck else {'match_all':{}};
    print(scr_query);
    #scr_query = { "ids": { "values": _ids } } if _ids else { 'bool':{'must_not':  {'term':{'has_'+field: True}}                                                                    } } if not recheck else {'bool':{'must':{'term':{'has_'+id_field+'s': True}}}};
    #scr_query = { "ids": { "values": _ids } } if _ids else { 'bool':{'must_not': [{'term':{'has_'+field: True}}], 'should': [{'term':{'has_'+refobj:True}} for refobj in _refobjs] } } if not recheck else {'bool':{'must':{'term':{'has_'+id_field+'s': True}}}};
    con = sqlite3.connect('urls_'+index+'.db');# if USE_BUFFER else None;
    cur = con.cursor();# if BUFFER else None;
    #if BUFFER:
    cur.execute("CREATE TABLE IF NOT EXISTS urls(url TEXT PRIMARY KEY, status INTEGER, resolve TEXT)");
    #----------------------------------------------------------------------------------------------------------------------------------
    client   = ES(['http://localhost:9200'],timeout=60);#ES(['localhost'],scheme='http',port=9200,timeout=60);print(scr_query)
    page     = client.search(index=index,scroll=str(int(_max_extract_time*_scroll_size))+'m',size=_scroll_size,query=scr_query);
    sid      = page['_scroll_id'];
    returned = len(page['hits']['hits']);
    page_num = 0;
    while returned > 0:
        for doc in page['hits']['hits']:
            #print('---------------------------------------------------------------------------------------------\n',doc['_id'],'---------------------------------------------------------------------------------------------\n');
            body        = copy(body);
            body['_id'] = doc['_id'];
            ids         = set(doc['_source'][field]) if field in doc['_source'] and doc['_source'][field] != None else set([]);
            for refobj in _refobjs:
                previous_refobjects                      = doc['_source'][refobj] if refobj in doc['_source'] and doc['_source'][refobj] else None;
                new_ids, new_refobjects                  = get_url(previous_refobjects,field,id_field,cur,USE_BUFFER) if isinstance(previous_refobjects,list) else (set([]),previous_refobjects);
                ids                                     |= new_ids; #TODO: Switched back to |= as I dont get it #This used to be |= but now the functionality is different
                body['_source']['doc'][refobj]           = new_refobjects; # The updated ones
                body['_source']['doc'][field+'_'+refobj] = list(new_ids);
                #print('-->',refobj,'gave',['','no '][len(new_ids)==0]+'ids',', '.join(new_ids),'\n');
            #print('------------------------------------------------\n-- overall ids --------------------------------\n'+', '.join(ids)+'\n------------------------------------------------');
            body['_source']['doc'][field]              = list(ids);
            body['_source']['doc']['processed_'+field] = True;
            body['_source']['doc']['has_'+field]       = len(ids) > 0;
            body['_source']['doc']['num_'+field]       = len(ids); print('-->','num_'+field,body['_source']['doc']['num_'+field])
            con.commit();
            yield body;
        scroll_tries = 0;
        while scroll_tries < _max_scroll_tries:
            try:
                page      = client.scroll(scroll_id=sid, scroll=str(int(_max_extract_time*_scroll_size))+'m');
                returned  = len(page['hits']['hits']);
                page_num += 1;
            except Exception as e:
                print(e, file=sys.stderr);
                print('\n[!]-----> Some problem occured while scrolling. Sleeping for 3s and retrying...\n');
                returned      = 0;
                scroll_tries += 1;
                time.sleep(3); continue;
            break;
        #if USE_BUFFER:
        con.commit();
    #if USE_BUFFER:
    con.close();
    client.clear_scroll(scroll_id=sid);
#-------------------------------------------------------------------------------------------------------------------------------------------------
