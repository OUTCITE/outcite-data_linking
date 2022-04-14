import sys
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
import sqlite3
import time
from tabulate import tabulate

_max_extract_time = 0.5; #minutes
_max_scroll_tries = 2;
_scroll_size      = 100;

_index         = sys.argv[1];
_outDB         = sys.argv[2];
_chunk_size    = 1000;

_refobjs = [    'anystyle_references_from_cermine_fulltext',
                'anystyle_references_from_cermine_refstrings',
                'anystyle_references_from_grobid_fulltext',
                'anystyle_references_from_grobid_refstrings',   #                'anystyle_references_from_gold_fulltext',
                'cermine_references_from_cermine_xml',          #                'anystyle_references_from_gold_refstrings',
                'cermine_references_from_grobid_refstrings',    #                'cermine_references_from_gold_refstrings',
                'grobid_references_from_grobid_xml' ];

_ids = None;#["EiGe_1976_0001"];

_load_links      = True;  # Matching information and URL information
_load_references = False; # Metadata of the extracted references
_load_metadata   = True;  # Metadata of the matched target objects


def get_links(index,refobj):
    #----------------------------------------------------------------------------------------------------------------------------------
    scr_body = {'query': { 'match_all':{} }, '_source':['@id',refobj] };
    #----------------------------------------------------------------------------------------------------------------------------------
    client   = ES(['localhost'],scheme='http',port=9200,timeout=60);
    page     = client.search(index=index,scroll=str(int(_max_extract_time*_scroll_size))+'m',size=_scroll_size,body=scr_body);
    sid      = page['_scroll_id'];
    returned = len(page['hits']['hits']);
    page_num = 0;
    while returned > 0:
        for doc in page['hits']['hits']:
            if refobj in doc['_source'] and isinstance(doc['_source'][refobj],list):
                for i in range(len(doc['_source'][refobj])):
                    reference                                                 = doc['_source'][refobj][i];
                    fromID                                                    = doc['_source']['@id'];
                    linkID                                                    = fromID+'_ref_'+str(i);
                    toID_sowiport,  toID_crossref,  toID_dnb,  toID_openalex  = None, None, None, None;
                    toURL_sowiport, toURL_crossref, toURL_dnb, toURL_openalex = None, None, None, None;
                    if 'sowiport_id' in reference and reference['sowiport_id']:
                        toID_sowiport  = reference['sowiport_id'];
                        toURL_sowiport = reference['sowiport_url'] if 'sowiport_url' in reference else None;
                    if 'crossref_id' in reference and reference['crossref_id']:
                        toID_crossref  = reference['crossref_id'];
                        toURL_crossref = reference['crossref_url'] if 'crossref_url' in reference else None;
                    if 'dnb_id' in reference and reference['dnb_id']:
                        toID_dnb  = reference['dnb_id'];
                        toURL_dnb = reference['dnb_url'] if 'dnb_url' in reference else None;
                    if 'openalex_id' in reference and reference['openalex_id']:
                        toID_openalex  = reference['openalex_id'];
                        toURL_openalex = reference['openalex_url'] if 'openalex_url' in reference else None;
                    if toID_sowiport or toID_crossref or toID_dnb or toID_openalex:
                        yield (linkID,fromID,toID_sowiport,toURL_sowiport,toID_crossref,toURL_crossref,toID_dnb,toURL_dnb,toID_openalex,toURL_openalex,);
        scroll_tries = 0;
        while scroll_tries < _max_scroll_tries:
            try:
                page      = client.scroll(scroll_id=sid, scroll=str(int(_max_extract_time*_scroll_size))+'m');
                returned  = len(page['hits']['hits']);
                page_num += 1;
            except Exception as e:
                print(e);
                print('\n[!]-----> Some problem occured while scrolling. Sleeping for 3s and retrying...\n');
                returned      = 0;
                scroll_tries += 1;
                time.sleep(3); continue;
            break;

def get_metadata(index,refobj):
    #----------------------------------------------------------------------------------------------------------------------------------
    scr_body = {'query': { 'match_all':{} }, '_source':['@id',refobj] };
    #----------------------------------------------------------------------------------------------------------------------------------
    client   = ES(['localhost'],scheme='http',port=9200,timeout=60);
    page     = client.search(index=index,scroll=str(int(_max_extract_time*_scroll_size))+'m',size=_scroll_size,body=scr_body);
    sid      = page['_scroll_id'];
    returned = len(page['hits']['hits']);
    page_num = 0;
    while returned > 0:
        for doc in page['hits']['hits']:
            if refobj in doc['_source'] and isinstance(doc['_source'][refobj],list):
                for i in range(len(doc['_source'][refobj])):
                    reference = doc['_source'][refobj][i];
                    fromID    = doc['_source']['@id'];
                    linkID    = fromID+'_ref_'+str(i);
                    issue     = reference['issue']                             if 'issue'      in reference else None;
                    volume    = reference['volume']                            if 'volume'     in reference else None;
                    year      = reference['year']                              if 'year'       in reference else None;
                    source    = reference['source']                            if 'source'     in reference else None;
                    title     = reference['title']                             if 'title'      in reference else None;
                    typ       = reference['type']                              if 'type'       in reference else None;
                    refstr    = reference['reference']                         if 'reference'  in reference else None;
                    startp    = reference['start']                             if 'start'      in reference else None;
                    endp      = reference['end']                               if 'end'        in reference else None;
                    place     = reference['place']                             if 'place'      in reference else None;
                    author1   = reference['authors'][0]['author_string']       if 'authors'    in reference and isinstance(reference['authors'],list) and 'author_string' in reference['authors'][0] else None;
                    publor1   = reference['publishers'][0]['publisher_string'] if 'publishers' in reference and isinstance(reference['publishers'],list) and 'publisher_string' in reference['publishers'][0] else None;
                    editor1   = reference['editors'][0]['editor_string']       if 'editors'    in reference and isinstance(reference['editors'],list) and 'editor_string' in reference['editors'][0] else None;
                    yield (linkID,fromID,issue,volume,year,source,title,typ,refstr,startp,endp,author1,publor1,editor1,);
        scroll_tries = 0;
        while scroll_tries < _max_scroll_tries:
            try:
                page      = client.scroll(scroll_id=sid, scroll=str(int(_max_extract_time*_scroll_size))+'m');
                returned  = len(page['hits']['hits']);
                page_num += 1;
            except Exception as e:
                print(e);
                print('\n[!]-----> Some problem occured while scrolling. Sleeping for 3s and retrying...\n');
                returned      = 0;
                scroll_tries += 1;
                time.sleep(3); continue;
            break;

def get_target(index,ID):
    #----------------------------------------------------------------------------------------------------------------------------------
    search_body = {'query': { 'ids': { 'values':[ID] } } };
    #----------------------------------------------------------------------------------------------------------------------------------
    client   = ES(['localhost'],scheme='http',port=9200,timeout=60);
    page     = client.search(index=index,body=search_body);
    returned = len(page['hits']['hits']);
    if returned == 0:
        return None;
    reference                                                  = page['hits']['hits'][0]['_source'];
    issue,volume,year,source,title,typ,author1,publor1,editor1 = None,None,None,None,None,None,None,None,None;
    if index == 'sowiport':
        year      = reference['date']          if 'date'          in reference                                              else None;
        source    = reference['source']        if 'source'        in reference                                              else None;
        title     = reference['title']         if 'title'         in reference                                              else None;
        typ       = reference['subtype']       if 'subtype'       in reference                                              else None;
        author1   = reference['coreAuthor'][0] if 'coreAuthor'    in reference and isinstance(reference['coreAuthor'],list) else None;
        publor1   = reference['corePublisher'] if 'corePublisher' in reference                                              else None;
        editor1   = reference['coreEditor'][0] if 'coreEditor'    in reference and isinstance(reference['coreEditor'],list) else None;
    elif index == 'crossref':
        issue     = reference['issue']                                               if 'issue'           in reference                                                                                                                       else None;
        volume    = reference['volume']                                              if 'volume'          in reference                                                                                                                       else None;
        year      = reference['published-print']['date-parts']                       if 'published-print' in reference and 'date-parts' in reference['published-print']                                                                      else None;
        title     = reference['title'][0]                                            if 'title'           in reference and isinstance(reference['title'],list)                                                                               else None;
        typ       = reference['type']                                                if 'type'            in reference                                                                                                                       else None;
        author1   = reference['author'][0]['given']+reference['author'][0]['family'] if 'author'          in reference and isinstance(reference['author'],list) and 'given' in reference['author'][0] and 'family' in reference['author'][0] else None;
        publor1   = reference['publisher']                                           if 'publisher'       in reference                                                                                                                       else None;
    elif index == 'dnb':
        year      = reference['pub_dates'][0]  if 'pub_dates'  in reference and isinstance(reference['pub_dates'],list)   else None;
        title     = reference['title']         if 'title'      in reference                                              else None;
        author1   = reference['authors'][0]    if 'authors'    in reference and isinstance(reference['authors'],list)    else None;
        publor1   = reference['publishers'][0] if 'publishers' in reference and isinstance(reference['publishers'],list) else None;
    elif index == 'openalex':
        issue     = reference['biblio']['issue']                if 'biblio'           in reference and 'issue'  in reference['biblio']                                                                                                 else None;
        volume    = reference['biblio']['volume']               if 'biblio'           in reference and 'volume' in reference['biblio']                                                                                                 else None;
        year      = reference['publication_year']               if 'publication_year' in reference                                                                                                                                     else None;
        source    = reference['host_venue']['display_name']     if 'host_venue'       in reference and 'display_name' in reference['host_venue']                                                                                       else None;
        title     = reference['title']                          if 'title'            in reference                                                                                                                                     else None;
        typ       = reference['type']                           if 'type'             in reference                                                                                                                                     else None;
        author1   = reference['authorships'][0]['display_name'] if 'authorships'      in reference and isinstance(reference['authorships'],list) and len(reference['authorships'])>0 and 'display_name' in reference['authorships'][0] else None;
        publor1   = reference['host_venue']['publisher']        if 'host_venue'       in reference and 'publisher' in reference['host_venue']                                                                                          else None;
    try:
        volume = int(volume);
    except:
        volume = None;
    try:
        issue = int(issue);
    except:
        issue = None;
    try:
        year = int(year);
    except:
        year = None;
    return (issue,volume,year,source,title,typ,author1,publor1,editor1,);


def get_targets(index,refobj):
    #----------------------------------------------------------------------------------------------------------------------------------
    scr_body = {'query': { 'match_all':{} }, '_source':['@id',refobj] };
    #----------------------------------------------------------------------------------------------------------------------------------
    client   = ES(['localhost'],scheme='http',port=9200,timeout=60);
    page     = client.search(index=index,scroll=str(int(_max_extract_time*_scroll_size))+'m',size=_scroll_size,body=scr_body);
    sid      = page['_scroll_id'];
    returned = len(page['hits']['hits']);
    page_num = 0;
    while returned > 0:
        for doc in page['hits']['hits']:
            if refobj in doc['_source'] and isinstance(doc['_source'][refobj],list):
                for i in range(len(doc['_source'][refobj])):
                    reference                                                 = doc['_source'][refobj][i];
                    toID_sowiport,  toID_crossref,  toID_dnb,  toID_openalex  = None, None, None, None;
                    toURL_sowiport, toURL_crossref, toURL_dnb, toURL_openalex = None, None, None, None;
                    if 'sowiport_id' in reference and reference['sowiport_id']:
                        toID_sowiport  = reference['sowiport_id'];
                        toURL_sowiport = reference['sowiport_url'] if 'sowiport_url' in reference else None;
                    if 'crossref_id' in reference and reference['crossref_id']:
                        toID_crossref  = reference['crossref_id'];
                        toURL_crossref = reference['crossref_url'] if 'crossref_url' in reference else None;
                    if 'dnb_id' in reference and reference['dnb_id']:
                        toID_dnb  = reference['dnb_id'];
                        toURL_dnb = reference['dnb_url'] if 'dnb_url' in reference else None;
                    if 'openalex_id' in reference and reference['openalex_id']:
                        toID_openalex  = reference['openalex_id'];
                        toURL_openalex = reference['openalex_url'] if 'openalex_url' in reference else None;
                    for toID, toCollection in [(toID_sowiport,'sowiport',),(toID_crossref,'crossref',),(toID_dnb,'dnb',),(toID_openalex,'openalex',)]:
                        if not toID:
                            continue;
                        result = get_target(toCollection,toID);
                        if result != None:
                            issue,volume,year,source,title,typ,author1,publor1,editor1 = result;
                            yield (toID,toCollection,issue,volume,year,source,title,typ,author1,publor1,editor1,);
        scroll_tries = 0;
        while scroll_tries < _max_scroll_tries:
            try:
                page      = client.scroll(scroll_id=sid, scroll=str(int(_max_extract_time*_scroll_size))+'m');
                returned  = len(page['hits']['hits']);
                page_num += 1;
            except Exception as e:
                print(e);
                print('\n[!]-----> Some problem occured while scrolling. Sleeping for 3s and retrying...\n');
                returned      = 0;
                scroll_tries += 1;
                time.sleep(3); continue;
            break;

def get_matches(index,refobj):
    #----------------------------------------------------------------------------------------------------------------------------------
    scr_body = {'query': { 'match_all':{} }, '_source':['@id',refobj,'results_'+refobj]  };
    #----------------------------------------------------------------------------------------------------------------------------------
    client   = ES(['localhost'],scheme='http',port=9200,timeout=60);
    page     = client.search(index=index,scroll=str(int(_max_extract_time*_scroll_size))+'m',size=_scroll_size,body=scr_body);
    sid      = page['_scroll_id'];
    returned = len(page['hits']['hits']);
    page_num = 0;
    while returned > 0:
        for doc in page['hits']['hits']:
            if 'results_'+refobj in doc['_source'] and isinstance(doc['_source']['results_'+refobj],dict) and 'refobj' in doc['_source']['results_'+refobj] and isinstance(doc['_source']['results_'+refobj]['refobj'],dict) and isinstance(doc['_source']['results_'+refobj]['refobj']['matches'],list):
                for i in range(len(doc['_source']['results_'+refobj]['refobj']['matches'])):
                    fromID                                                    = doc['_source']['@id'];
                    linkID                                                    = fromID+'_ref_'+str(i);
                    reference                                                 = doc['_source'][refobj][i];
                    toID_sowiport,  toID_crossref,  toID_dnb,  toID_openalex  = None, None, None, None;
                    if 'sowiport_id' in reference and reference['sowiport_id']:
                        toID_sowiport  = reference['sowiport_id'];
                    if 'crossref_id' in reference and reference['crossref_id']:
                        toID_crossref  = reference['crossref_id'];
                    if 'dnb_id' in reference and reference['dnb_id']:
                        toID_dnb  = reference['dnb_id'];
                    if 'openalex_id' in reference and reference['openalex_id']:
                        toID_openalex  = reference['openalex_id'];
                    for matchType, fromMatch, toMatch in doc['_source']['results_'+refobj]['refobj']['matches'][i]:
                        yield (linkID,fromID,toID_sowiport,toID_crossref,toID_dnb,toID_openalex,fromMatch,toMatch,matchType,);
        scroll_tries = 0;
        while scroll_tries < _max_scroll_tries:
            try:
                page      = client.scroll(scroll_id=sid, scroll=str(int(_max_extract_time*_scroll_size))+'m');
                returned  = len(page['hits']['hits']);
                page_num += 1;
            except Exception as e:
                print(e);
                print('\n[!]-----> Some problem occured while scrolling. Sleeping for 3s and retrying...\n');
                returned      = 0;
                scroll_tries += 1;
                time.sleep(3); continue;
            break;

def get_mismatches(index,refobj):
    #----------------------------------------------------------------------------------------------------------------------------------
    scr_body = {'query': { 'match_all':{} }, '_source':['@id',refobj,'results_'+refobj]  };
    #----------------------------------------------------------------------------------------------------------------------------------
    client   = ES(['localhost'],scheme='http',port=9200,timeout=60);
    page     = client.search(index=index,scroll=str(int(_max_extract_time*_scroll_size))+'m',size=_scroll_size,body=scr_body);
    sid      = page['_scroll_id'];
    returned = len(page['hits']['hits']);
    page_num = 0;
    while returned > 0:
        for doc in page['hits']['hits']:
            if 'results_'+refobj in doc['_source'] and isinstance(doc['_source']['results_'+refobj],dict) and 'refobj' in doc['_source']['results_'+refobj] and isinstance(doc['_source']['results_'+refobj]['refobj'],dict) and isinstance(doc['_source']['results_'+refobj]['refobj']['mismatches'],list):
                for i in range(len(doc['_source']['results_'+refobj]['refobj']['mismatches'])):
                    fromID                                                    = doc['_source']['@id'];
                    linkID                                                    = fromID+'_ref_'+str(i);
                    reference                                                 = doc['_source'][refobj][i];
                    toID_sowiport,  toID_crossref,  toID_dnb,  toID_openalex  = None, None, None, None;
                    if 'sowiport_id' in reference and reference['sowiport_id']:
                        toID_sowiport  = reference['sowiport_id'];
                    if 'crossref_id' in reference and reference['crossref_id']:
                        toID_crossref  = reference['crossref_id'];
                    if 'dnb_id' in reference and reference['dnb_id']:
                        toID_dnb  = reference['dnb_id'];
                    if 'openalex_id' in reference and reference['openalex_id']:
                        toID_openalex  = reference['openalex_id'];
                    if toID_sowiport or toID_crossref or toID_dnb or toID_openalex:
                        for matchType, fromMatch, toMatch in doc['_source']['results_'+refobj]['refobj']['mismatches'][i]:
                            yield (linkID,fromID,toID_sowiport,toID_crossref,toID_dnb,toID_openalex,fromMatch,toMatch,matchType,);
        scroll_tries = 0;
        while scroll_tries < _max_scroll_tries:
            try:
                page      = client.scroll(scroll_id=sid, scroll=str(int(_max_extract_time*_scroll_size))+'m');
                returned  = len(page['hits']['hits']);
                page_num += 1;
            except Exception as e:
                print(e);
                print('\n[!]-----> Some problem occured while scrolling. Sleeping for 3s and retrying...\n');
                returned      = 0;
                scroll_tries += 1;
                time.sleep(3); continue;
            break;

#-------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------

_con = sqlite3.connect(_outDB);
_cur = _con.cursor();

_client = ES(['localhost'],scheme='http',port=9200,timeout=60);

if _load_links:
    for refobj in _refobjs:
        print('Loading links from '+refobj+'...');
        _cur.execute("DROP   TABLE IF EXISTS links_"+refobj);
        _cur.execute("CREATE TABLE           links_"+refobj+"(linkID TEXT PRIMARY KEY, fromID TEXT, toID_sowiport TEXT, toURL_sowiport TEXT, toID_crossref TEXT, toURL_crossref TEXT, toID_dnb TEXT, toURL_dnb TEXT, toID_openalex TEXT, toURL_openalex TEXT)");
        _cur.executemany("INSERT INTO links_"+refobj+" VALUES(?,?,?,?,?,?,?,?,?,?)",get_links(_index,refobj));
    _con.commit();

if _load_references:
    for refobj in _refobjs:
        print('Loading reference metadata from '+refobj+'...');
        _cur.execute("DROP   TABLE IF EXISTS metadata_"+refobj);
        _cur.execute("CREATE TABLE           metadata_"+refobj+"(linkID TEXT PRIMARY KEY, fromID TEXT, issue TEXT, volume INT, year INT, source TEXT, title TEXT, type TEXT, refstr TEXT, startp INT, endp INT, author1 TEXT, publisher1 TEXT, editor1 TEXT)");
        _cur.executemany("INSERT INTO metadata_"+refobj+" VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",get_metadata(_index,refobj));
    _con.commit();

if _load_metadata:
    _cur.execute("DROP   TABLE IF EXISTS targets");
    _cur.execute("CREATE TABLE           targets(toID TEXT PRIMARY KEY, toCollection TEXT, issue TEXT, volume INT, year INT, source TEXT, title TEXT, type TEXT, author1 TEXT, publisher1 TEXT, editor1 TEXT)");
    for refobj in _refobjs:
        print('Loading target metadata from '+refobj+'...');
        _cur.executemany("INSERT OR IGNORE INTO targets VALUES(?,?,?,?,?,?,?,?,?,?,?)",get_targets(_index,refobj));
    _con.commit();

#-MATCHES----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
table_numb = [];
table_prob = [];
for a in ['sowiport','crossref','dnb','openalex']:
    for has_a in [True,False]:
        row_numb = [['-','+'][has_a]+a];
        row_prob = [['-','+'][has_a]+a];
        denominator = _cur.execute("SELECT COUNT(*) FROM links_grobid_references_from_grobid_xml WHERE toID_"+a+" IS "+['','NOT'][has_a]+" NULL").fetchall()[0][0];
        for has_b in [True,False]:
            for b in ['sowiport','crossref','dnb','openalex']:
                numerator = _cur.execute("SELECT COUNT(*) FROM links_grobid_references_from_grobid_xml WHERE toID_"+a+" IS "+['','NOT'][has_a]+" NULL AND toID_"+b+" IS "+['','NOT'][has_b]+" NULL").fetchall()[0][0];
                row_numb.append(numerator);
                row_prob.append(round(100*numerator/denominator,1) if denominator>0 else 0);
        table_numb.append(copy(row_numb));
        table_prob.append(copy(row_prob));

table_numb = [table_numb[0],table_numb[2],table_numb[4],table_numb[6],table_numb[1],table_numb[3],table_numb[5],table_numb[7]];
table_prob = [table_prob[0],table_prob[2],table_prob[4],table_prob[6],table_prob[1],table_prob[3],table_prob[5],table_prob[7]];
print(tabulate(table_numb,headers=[_index]+['+sowiport','+crossref','+dnb','+openalex']+['-sowiport','-crossref','-dnb','-openalex']))
print(tabulate(table_prob,headers=[_index]+['+sowiport','+crossref','+dnb','+openalex']+['-sowiport','-crossref','-dnb','-openalex']))
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#-URLS-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
table_numb = [];
table_prob = [];
for a in ['sowiport','crossref','dnb','openalex']:
    for has_a in [True,False]:
        row_numb = [['-','+'][has_a]+a];
        row_prob = [['-','+'][has_a]+a];
        denominator = _cur.execute("SELECT COUNT(*) FROM links_grobid_references_from_grobid_xml WHERE toURL_"+a+" IS "+['','NOT'][has_a]+" NULL").fetchall()[0][0];
        for has_b in [True,False]:
            for b in ['sowiport','crossref','dnb','openalex']:
                numerator = _cur.execute("SELECT COUNT(*) FROM links_grobid_references_from_grobid_xml WHERE toURL_"+a+" IS "+['','NOT'][has_a]+" NULL AND toURL_"+b+" IS "+['','NOT'][has_b]+" NULL").fetchall()[0][0];
                row_numb.append(numerator);
                row_prob.append(round(100*numerator/denominator,1) if denominator>0 else 0);
        table_numb.append(copy(row_numb));
        table_prob.append(copy(row_prob));

table_numb = [table_numb[0],table_numb[2],table_numb[4],table_numb[6],table_numb[1],table_numb[3],table_numb[5],table_numb[7]];
table_prob = [table_prob[0],table_prob[2],table_prob[4],table_prob[6],table_prob[1],table_prob[3],table_prob[5],table_prob[7]];
print(tabulate(table_numb,headers=[_index]+['+sowiport','+crossref','+dnb','+openalex']+['-sowiport','-crossref','-dnb','-openalex']))
print(tabulate(table_prob,headers=[_index]+['+sowiport','+crossref','+dnb','+openalex']+['-sowiport','-crossref','-dnb','-openalex']))
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#-IDS VS URLS------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
table_numb = [];
table_prob = [];
for a in ['sowiport','crossref','dnb','openalex']:
    for has_a in [True,False]:
        row_numb = [['-','+'][has_a]+a];
        row_prob = [['-','+'][has_a]+a];
        denominator = _cur.execute("SELECT COUNT(*) FROM links_grobid_references_from_grobid_xml WHERE toID_"+a+" IS "+['','NOT'][has_a]+" NULL").fetchall()[0][0];
        for has_b in [True,False]:
            for b in ['sowiport','crossref','dnb','openalex']:
                numerator = _cur.execute("SELECT COUNT(*) FROM links_grobid_references_from_grobid_xml WHERE toID_"+a+" IS "+['','NOT'][has_a]+" NULL AND toURL_"+b+" IS "+['','NOT'][has_b]+" NULL").fetchall()[0][0];
                row_numb.append(numerator);
                row_prob.append(round(100*numerator/denominator,1) if denominator>0 else 0);
        table_numb.append(copy(row_numb));
        table_prob.append(copy(row_prob));

table_numb = [table_numb[0],table_numb[2],table_numb[4],table_numb[6],table_numb[1],table_numb[3],table_numb[5],table_numb[7]];
table_prob = [table_prob[0],table_prob[2],table_prob[4],table_prob[6],table_prob[1],table_prob[3],table_prob[5],table_prob[7]];
print(tabulate(table_numb,headers=[_index]+['+sowiport','+crossref','+dnb','+openalex']+['-sowiport','-crossref','-dnb','-openalex']))
print(tabulate(table_prob,headers=[_index]+['+sowiport','+crossref','+dnb','+openalex']+['-sowiport','-crossref','-dnb','-openalex']))
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

if _index == 'geocite' or _index == 'ssoar_gold':

    for refobj in _refobjs:
        print('Loading matches from '+refobj+'...');
        _cur.execute("DROP   TABLE IF EXISTS matches_"+refobj);
        _cur.execute("CREATE TABLE           matches_"+refobj+"(linkID TEXT, fromID TEXT, toID_sowiport TEXT, toID_crossref TEXT, toID_dnb TEXT, toID_openalex TEXT, fromMatch TEXT, toMatch TEXT, matchType TEXT)");
        _cur.executemany("INSERT INTO matches_"+refobj+" VALUES(?,?,?,?,?,?,?,?,?)",get_matches(_index,refobj));
    _con.commit();

    for refobj in _refobjs:
        print('Loading mismatches from '+refobj+'...');
        _cur.execute("DROP   TABLE IF EXISTS mismatches_"+refobj);
        _cur.execute("CREATE TABLE           mismatches_"+refobj+"(linkID TEXT, fromID TEXT, toID_sowiport TEXT, toID_crossref TEXT, toID_dnb TEXT, toID_openalex TEXT, fromMatch TEXT, toMatch TEXT, matchType TEXT)");
        _cur.executemany("INSERT INTO mismatches_"+refobj+" VALUES(?,?,?,?,?,?,?,?,?)",get_mismatches(_index,refobj));
    _con.commit();

_con.close();
#-------------------------------------------------------------------------------------------------------------------------------------------------
