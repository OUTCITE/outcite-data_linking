from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
import requests
import time
import sys
import json
from pathlib import Path
import re
import sqlite3

ARXIVID = re.compile("[0-9]+\.[0-9]+");

IN = None;
try:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs_custom.json');
except:
    IN = open(str((Path(__file__).parent / '../code/').resolve())+'/configs.json');
_configs = json.load(IN);
IN.close();

_max_extract_time = _configs['max_extract_time']; #minutes
_max_scroll_tries = _configs['max_scroll_tries'];
_scroll_size      = _configs['scroll_size'];

_refobjs = _configs['refobjs'];

_ids = _configs['ids'];

def check(url,RESOLVE=False,cur=None,timeout=20):
    print('Checking URL',url,'...');
    page   = None;
    status = None;
    try:
        status = None;
        if cur:
            rows    = cur.execute("SELECT status,resolve FROM urls WHERE url=?",(url,)).fetchall();
            status  = rows[0][0] if rows and rows[0] else None;
            new_url = rows[0][1] if rows and rows[0] else None;
        if not status:
            page    = requests.head(url,allow_redirects=True,timeout=timeout) if RESOLVE else requests.head(url,timeout=timeout);
            status  = page.status_code;
            new_url = page.url;
            if cur:
                cur.execute("INSERT INTO urls VALUES(?,?,?)",(url,status,new_url,));
        if status == 404:
            print('----> Could not resolve URL due to 404',url);
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

def extract_arxiv_id(string):
    ids = [match.group() for match in ARXIVID.finditer(string)];
    return ids[0] if ids else None;

def doi2url(doi,cur=None):
    url   = 'https://doi.org/'+doi;
    return check(url,True,cur);

def doi2url_(doi):
    doi   = 'https://doi.org/'+doi;
    url   = None;
    tries = 0;
    while True:
        try:
            print('Checking DOI',doi,'...');
            url = requests.head(doi,timeout=20).url;
            print('Done checking.');
            print(doi,url);
            break;
        except Exception as e:
            print(e, file=sys.stderr);
            if tries > _max_scroll_tries:
                print(e);
                print('Problem obtaining URL for doi '+doi+'. Giving up.');
                break;
            tries += 1;
            print(e);
            print('Problem obtaining URL for doi '+doi+'. Retrying...');
    return url;

def search(field,id_field,index,recheck,get_url,BUFFER=False):
    #----------------------------------------------------------------------------------------------------------------------------------
    body      = { '_op_type': 'update', '_index': index, '_id': None, '_source': { 'doc': { 'processed_'+field: True, field: None } } };
    scr_query = { "ids": { "values": _ids } } if _ids else { 'bool':{'must_not':  {'term':{'processed_'+field: True}}, 'must': {'term':{'has_'+id_field+'s':True}} } } if not recheck else {'bool':{'must':{'term':{'has_'+id_field+'s': True}}}};
    if id_field=='doi':
        scr_query = { "ids": { "values": _ids } } if _ids else { 'bool':{'must_not':  {'term':{'processed_'+field: True}}}} if not recheck else {'match_all':{}};
    #scr_query = { "ids": { "values": _ids } } if _ids else { 'bool':{'must_not':  {'term':{'has_'+field: True}}                                                                    } } if not recheck else {'bool':{'must':{'term':{'has_'+id_field+'s': True}}}};
    #scr_query = { "ids": { "values": _ids } } if _ids else { 'bool':{'must_not': [{'term':{'has_'+field: True}}], 'should': [{'term':{'has_'+refobj:True}} for refobj in _refobjs] } } if not recheck else {'bool':{'must':{'term':{'has_'+id_field+'s': True}}}};
    con = sqlite3.connect('urls_'+index+'.db') if BUFFER else None;
    cur = con.cursor() if con else None;
    if cur:
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
                new_ids, new_refobjects                  = get_url(previous_refobjects,field,id_field,cur) if isinstance(previous_refobjects,list) else (set([]),previous_refobjects);
                ids                                     |= new_ids;
                body['_source']['doc'][refobj]           = new_refobjects; # The updated ones
                body['_source']['doc'][field+'_'+refobj] = list(new_ids);
                #print('-->',refobj,'gave',['','no '][len(new_ids)==0]+'ids',', '.join(new_ids),'\n');
            #print('------------------------------------------------\n-- overall ids --------------------------------\n'+', '.join(ids)+'\n------------------------------------------------');
            body['_source']['doc'][field]              = list(ids);
            body['_source']['doc']['processed_'+field] = True;
            body['_source']['doc']['has_'+field]       = len(ids) > 0;
            body['_source']['doc']['num_'+field]       = len(ids); print('-->',body['_source']['doc']['num_'+field])
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
        con.commit();
    con.close();
    client.clear_scroll(scroll_id=sid);
