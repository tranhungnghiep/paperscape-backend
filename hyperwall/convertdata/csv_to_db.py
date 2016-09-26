"""
purpose of csv_to_db.py: convert MPDL csv data into database tables
"""

# PYTHON3
import platform
assert platform.python_version_tuple()[0] == '3'

import math
import sys
import argparse
import datetime
import pandas as pd
import numpy as np

from pciteblob import buildBlob, decodeBlob
import mysql

def create_new_meta_table(db_cursor, table_name):
    """
    Delete (drop) old meta table and create a new one.
    """

    cmd_drop = "DROP TABLE IF EXISTS {}".format(table_name)

    cmd_create = """
        CREATE TABLE {} (
            id INT(10) UNSIGNED PRIMARY KEY,
            year SMALLINT(4) UNSIGNED,
            numco SMALLINT UNSIGNED DEFAULT 0,
            cocodes TEXT,
            numcat SMALLINT UNSIGNED DEFAULT 0,
            catcodes TEXT,
            maincat VARCHAR(2) DEFAULT '',
            numbigcat SMALLINT UNSIGNED DEFAULT 0,
            bigcats TEXT,
            mainbigcat VARCHAR(32) DEFAULT '',
            publ VARCHAR(200),
            title VARCHAR(500),
            authors TEXT,
            keywords TEXT,
            nummpg SMALLINT UNSIGNED DEFAULT 0,
        ) ENGINE = MyISAM""".format(table_name)

    try:
        db_cursor.execute(cmd_drop, ())
    except Exception as e :
        print("MySQL error: {}".format(str(e)))
    try:
        db_cursor.execute(cmd_create, ())
    except Exception as e :
        print("MySQL error: {}".format(str(e)))


def create_new_cite_table(db_cursor, table_name):
    """
    Delete (drop) old cite table and create a new one.
    """

    cmd_drop = "DROP TABLE IF EXISTS {}".format(table_name)

    cmd_create = """
        CREATE TABLE {} (
            id INT(10) UNSIGNED PRIMARY KEY,
            numRefs INT(10) UNSIGNED DEFAULT 0, 
            refs BLOB,
            numCites INT(10) UNSIGNED DEFAULT 0, 
            cites MEDIUMBLOB
        ) ENGINE = MyISAM""".format(table_name)

    try:
        db_cursor.execute(cmd_drop, ())
    except Exception as e :
        print("MySQL error: {}".format(str(e)))
    try:
        db_cursor.execute(cmd_create, ())
    except Exception as e :
        print("MySQL error: {}".format(str(e)))

def create_new_abstract_table(db_cursor, table_name):
    """
    Delete (drop) old abstract table and create a new one.
    """

    cmd_drop = "DROP TABLE IF EXISTS {}".format(table_name)

    cmd_create = """
        CREATE TABLE {} (
            id INT(10) UNSIGNED PRIMARY KEY,
            abstract MEDIUMTEXT
        ) ENGINE = MyISAM""".format(table_name)

    try:
        db_cursor.execute(cmd_drop, ())
    except Exception as e :
        print("MySQL error: {}".format(str(e)))
    try:
        db_cursor.execute(cmd_create, ())
    except Exception as e :
        print("MySQL error: {}".format(str(e)))


def populate_meta_table(db_cursor, dry_run, meta_table, items_file, new_table=True,sample_size=-1):
    """
    Fill meta_table with items data i.e. id, year and country codes
    """

    if new_table and not dry_run :
        create_new_meta_table(db_cursor, meta_table)

    items = pd.read_csv(items_file,delim_whitespace=True,header=0,names=('id','year','cocodes'))
    items.sort_values('id',inplace=True)
    items.index = range(0,len(items))

    print("Populating meta_table")
    num_iters = len(items)
    if sample_size > 0 and sample_size < num_iters : num_iters = sample_size
    for i, row in items.iterrows() :
        if i >= num_iters : break
        print("\r{:2.0f}% done...".format(min(99,100*i/num_iters)),end='')
        
        item_id,publ_year,country_codes = row

        numco = 0
        if country_codes == '--' : 
            country_codes = ''
        else : 
            numco = len(country_codes.split(',')) 
        
        cmd = 'INSERT INTO {} (id,year,numco,cocodes) VALUES (%s,%s,%s,%s)'.format(meta_table)
        arg = (item_id,str(publ_year),numco,country_codes)
        if not mysql.execute_mutating_query(db_cursor, dry_run, cmd, arg):
            return
    print('\n')

def add_categories_to_meta_table(db_cursor,dry_run,meta_table,subjects_file,sample_size=-1):

    subjects = pd.read_csv(subjects_file,delim_whitespace=True,header=0,names=('id','cat'))
    subjects.sort_values('id',inplace=True)
    subjects.index = range(0,len(subjects))

    curr_id = -1
    cats    = []
    print("Adding subject categories to meta_table")
    if sample_size > 0 : 
        tot_subjects = sample_size
    else :
        tot_subjects = len(subjects)
    
    for i, row in subjects.iterrows() :
        if i > tot_subjects : break
        print("\r{:2.0f}% done...".format(min(99,100*i/tot_subjects)),end='')
        
        item_id, cat = row
        if i > 0 and item_id != curr_id :
            # arrived at new id, so record old one
            cmd = 'UPDATE {} SET numcat = %s, catcodes = %s WHERE id = {}'.format(meta_table,curr_id)
            arg = (len(cats),",".join(cats))
            if not mysql.execute_mutating_query(db_cursor, dry_run, cmd, arg):
                return
            
            cats = []
        
        cats.append(cat)
        curr_id = item_id
    print('\n')

def add_big_categories_to_meta_table(db_cursor, dry_run, meta_table, bigcat_file) :

    bigcats = pd.read_csv(bigcat_file,delim_whitespace=True,header=None,names=('cat','bigcat'))

    print("Adding big categories to meta_table")
    
    # make dictionary
    cats_dict = {}
    for i, row in bigcats.iterrows() :
        cat, bigcat = row
        cats_dict[cat.strip().lower()] = bigcat.strip().lower()

    print("Getting WOS categories from DB and finding big cats")
    ids = {}
    missing_cats = {}
    hits = db_cursor.execute('SELECT id,catcodes,numcat FROM {}'.format(meta_table))
    for i in range(hits) :
        print("\r{:2.0f}% done...".format(min(99,100*(i)/hits)),end='')
        id, catcodes, numcat = db_cursor.fetchone()
        
        bigcats = []
        if numcat > 0 and catcodes is not None and catcodes > "" :
            for cat in catcodes.split(',') :
                cat = cat.strip().lower()
                try : 
                    bigcat = cats_dict[cat]
                    bigcats.append(bigcat)
                except : 
                    missing_cats[cat] = True

        ids[id] = list(set(bigcats))

    print('\n')
    print("Missing cats: ",missing_cats)

    print("Inserting bigcats back into DB")
    i = 0
    for id,bigcats in ids.items() :
        print("\r{:2.0f}% done...".format(min(99,100*(i)/len(ids))),end='')
        i += 1
        if len(bigcats) > 0 :
            cmd = 'UPDATE {} SET bigcats = %s, numbigcat = %s WHERE id = %s'.format(meta_table)
            arg = (','.join(bigcats),len(bigcats),id)
            if not mysql.execute_mutating_query(db_cursor, dry_run, cmd, arg):
                return
            
    print('\n')

def add_refs_to_cite_table(db_cursor, dry_run, cite_table, xref_file, meta_table=None, new_table=True, sample_size=-1):

    if new_table and not dry_run :
        create_new_cite_table(db_cursor,cite_table)

    if sample_size > 0 :
        tot_xrefs = sample_size
    else :
        print("Counting length of references file: {}".format(xref_file))
        with open(xref_file) as f:
            tot_xrefs = sum(1 for _ in f)

    # read in manageable chunks and insert these
    # while checking if entry already exists in db
    print("Adding {} references to cites table".format(tot_xrefs))
    numchunks   = 10
    chunksize   = math.ceil(tot_xrefs/numchunks)
    chunk_index = 0
    print("Using {} chunks of size {}".format(numchunks,chunksize))

    for df in pd.read_csv(xref_file,delim_whitespace=True,chunksize=chunksize,iterator=True,header=0,names=('from','to')) :
        
        df.sort_values('from',inplace=True)
        df.index = range(0,len(df))

        curr_id = -1
        refs    = []
        for i, row in df.iterrows() :
            if i > tot_xrefs : break
            print("\r{:2.0f}% done... ({:2.0f}% for chunk {:2.0f} of {:2.0f})".format(min(99,100*(chunksize*chunk_index + i)/tot_xrefs),min(99,100*i/chunksize),chunk_index+1,numchunks),end='')
            
            from_id, to_id = row

            if i > 0 and from_id != curr_id :

                numRefs = len(refs)
                refsBlob = buildBlob(((id,0,0) for id in refs))
                
                if chunk_index > 0 or not new_table :
                    hits = db_cursor.execute('SELECT numRefs,refs FROM {} where id = {}'.format(cite_table,curr_id))
                    if hits  == 1 :
                        lookup_num_refs,lookup_refs = db_cursor.fetchone()
                        numRefs += lookup_num_refs
                        refsBlob =  lookup_refs + refsBlob

                cmd = 'INSERT INTO {} (id,numRefs,refs) VALUES (%s,%s,%s) ON DUPLICATE KEY UPDATE numRefs = VALUES(numRefs), refs = VALUES(refs)'.format(cite_table)
                arg = (int(curr_id),numRefs,bytes(refsBlob))
                if not mysql.execute_mutating_query(db_cursor, dry_run, cmd, arg):
                    return

                refs = []

            refs.append(to_id)
            curr_id = from_id

        chunk_index += 1
        #if chunk_index > numchunks-1 : break

    print('\n')

    if meta_table is not None :
        add_empty_papers_to_cite_table(db_cursor, dry_run, meta_table, cite_table)


def add_titles_to_meta_table(db_cursor, dry_run, meta_table, titles_file, idmap_file, sample_size=-1):
    """
    Add titles to meta table
    Uses idmap_file to match ids; original id is 'item_id', other is 'item_idx'
    """

    print("Reading in {}".format(idmap_file))
    idmap = pd.read_csv(idmap_file,sep='\t',header=0,names=('item_id','item_idx'))
    id_dict = dict(zip(idmap["item_idx"],idmap["item_id"]))
    del idmap

    print("Reading in {}".format(titles_file))
    titles = pd.read_csv(titles_file,sep='\t',header=0,names=('item_idx','type','title'),usecols=('item_idx','title'),quoting=3)
    #titles.sort_values('idx',inplace=True)
    titles.index = range(0,len(titles))

    print("Adding titles to {}".format(meta_table))
    num_ids_updated = 0
    num_missing_idx = 0
    max_str_len     = 0
    long_titles = []
    num_iters = len(titles)
    if sample_size > 0 and sample_size < num_iters : num_iters = sample_size
    for i, row in titles.iterrows() :
        if i >= num_iters : break
        print("\r{:2.0f}% done...".format(min(99,100*i/num_iters)),end='')
        
        item_idx, title = row
        try:
            item_id = id_dict[item_idx]
        except: 
            num_missing_idx += 1       
            continue

        max_str_len = max(max_str_len,len(title))
        if len(title) > 500:
            long_titles.append(item_id)

        cmd = 'UPDATE {} SET title = %s WHERE id = %s'.format(meta_table)
        arg = (title,str(item_id))
        if not mysql.execute_mutating_query(db_cursor, dry_run, cmd, arg):
            return
        num_ids_updated += 1
    print('\n')
   
    print(long_titles)
    print("{} too long titles".format(len(long_titles)))
    print("{} papers updated".format(num_ids_updated))
    print("Number of missing item_idx = {}".format(num_missing_idx))
    print("Maximum title string length = {}".format(max_str_len))

def add_authors_to_meta_table(db_cursor, dry_run, meta_table, authors_file, idmap_file, sample_size=-1):
    """
    Add authors to meta table
    Uses idmap_file to match ids
    """
    
    print("Reading in {}".format(idmap_file))
    idmap = pd.read_csv(idmap_file,sep='\t',header=0,names=('item_id','item_idx'))
    id_dict = dict(zip(idmap["item_idx"],idmap["item_id"]))
    del idmap

    print("Reading in {}".format(authors_file))
    authors = pd.read_csv(authors_file,sep='\t',header=0,names=('ITAU_ID','item_idx','position','author'),usecols=('item_idx','position','author'),quoting=3)
    authors.sort_values('item_idx',inplace=True)
    authors.index = range(0,len(authors))

    print("Adding authors to {}".format(meta_table))
    num_ids_updated = 0
    num_missing_idx = 0
    curr_idx = -1
    auths    = []
    num_iters = len(authors)
    if sample_size > 0 and sample_size < num_iters : num_iters = sample_size
    for i, row in authors.iterrows() :
        if i >= num_iters : break
        print("\r{:2.0f}% done...".format(min(99,100*i/num_iters)),end='')
        
        item_idx, pos, author = row
        #print("\n--- {} {} {}\n".format(item_idx,pos,author)) # for testing
        
        if i > 0 and item_idx != curr_idx and len(auths) > 0:
            # arrived at new paper id, so record old one
            try:
                item_id = id_dict[item_idx]
            except: 
                num_missing_idx += 1       
                #item_id = '1000000001' # for testing
                continue

            # order author array by position
            auths.sort(key=lambda x: x[1])

            cmd = 'UPDATE {} SET authors = %s WHERE id = %s'.format(meta_table)
            arg = (','.join([au for au, _ in auths]),str(item_id))
            if not mysql.execute_mutating_query(db_cursor, dry_run, cmd, arg):
                return
            num_ids_updated += 1
            auths = []
       
        # store authors xiwi style
        names = author.split(',')
        if len(names) > 1:
            # first names come in several styles, e.g.:
            # a -> A. ; a. r. -> A.R. ; andre f. -> A.F. ; jg -> J.G. ; yin -> Y. ; john -> J. ; h. larissa-emilia -> L.
            # i.e. two chars are assumed to be initials, 3 chars+ a name; ignore hyphens
            firstname = ""
            for n in names[1].replace(' ','.').split('.'):
                if n != "":
                    if len(n) == 2:
                        firstname += "{}.{}.".format(n[0].upper(),n[1].upper())
                    else:
                        firstname += n[0].strip().upper() + "."
            # capitalise first letter of lastname
            lastname = names[0][0].upper()
            if len(names[0]) > 1:
                lastname += names[0][1:]
            # combine into new name
            author = "{}{}".format(firstname,lastname)
        else:
            author = names[0]
        
        # store author along with position
        auths.append([author,pos])
        
        curr_idx = item_idx
    print('\n')

    print("{} papers updated".format(num_ids_updated))
    print("{} missing item_idx".format(num_missing_idx))

def add_keywords_to_meta_table(db_cursor, dry_run, meta_table, keywords_file, idmap_file, sample_size=-1):
    """
    Add keywords to meta table
    Uses idmap_file to match ids
    """

    print("Reading in {}".format(idmap_file))
    idmap = pd.read_csv(idmap_file,sep='\t',header=0,names=('item_id','item_idx'))
    id_dict = dict(zip(idmap["item_idx"],idmap["item_id"]))
    del idmap

    # NOTE ignoring keyword 'type' for now ('k' or 'p' ??)
    print("Reading in {}".format(keywords_file))
    keywords = pd.read_csv(keywords_file,sep='\t',header=0,names=('item_idx','keyword','type'),usecols=('item_idx','keyword'),quoting=3)
    keywords.sort_values('item_idx',inplace=True)
    keywords.index = range(0,len(keywords))

    print("Adding keywords to {}".format(meta_table))
    num_missing_idx = 0
    num_ids_updated = 0
    num_kw_updated  = 0
    curr_idx = -1
    kws      = []
    num_iters = len(keywords)
    if sample_size > 0 and sample_size < num_iters : num_iters = sample_size
    for i, row in keywords.iterrows() :
        if i >= num_iters : break
        print("\r{:2.0f}% done...".format(min(99,100*i/num_iters)),end='')
        
        item_idx, keyword = row
        #print("\n--- {} {}\n".format(item_idx,keyword)) # for testing
        
        if i > 0 and item_idx != curr_idx and len(kws) > 0:
            # arrived at new paper id, so record old one
            try:
                item_id = id_dict[item_idx]
            except: 
                num_missing_idx += 1       
                #item_id = '1000000001' # for testing
                continue
            
            cmd = 'UPDATE {} SET keywords = %s WHERE id = %s'.format(meta_table)
            arg = (','.join(kws),str(item_id))
            if not mysql.execute_mutating_query(db_cursor, dry_run, cmd, arg):
                return
            num_ids_updated += 1
            num_kw_updated += len(kws)
            kws = []
      
        # make sure keyword doesn't contain a comma
        keyword = keyword.replace(',',';')

        # store author along with position
        kws.append(keyword)
        
        curr_idx = item_idx
    print('\n')

    print("{} papers updated".format(num_ids_updated))
    print("average of {:.1f} keywords per paper".format(num_kw_updated/num_ids_updated))
    print("{} missing item_idx".format(num_missing_idx))

def add_publdata_to_meta_table(db_cursor, dry_run, meta_table, publdata_file, idmap_file, sample_size=-1):
    """
    Add publication data to meta table
    Uses idmap_file to match ids
    """
    
    print("Reading in {}".format(idmap_file))
    idmap = pd.read_csv(idmap_file,sep='\t',header=0,names=('item_id','item_idx'),quoting=3)
    id_dict = dict(zip(idmap["item_idx"],idmap["item_id"]))
    del idmap

    print("Reading in {}".format(publdata_file))
    publdata = pd.read_csv(publdata_file,sep='\t',header=0,names=('item_idx','ITEM_UT','ITEM_SRC','publ_year','ITEM_DT','doi','journal','ITEM_ARTN','ITEM_VL','ITEM_BP','num_mpg_affil'),usecols=('item_idx','publ_year','doi','journal','num_mpg_affil'),quoting=3)
    #publdata.sort_values('idx',inplace=True)
    publdata.index = range(0,len(publdata))

    print("Adding publdata to {}".format(meta_table))
    num_ids_updated = 0
    num_missing_idx = 0
    max_str_len     = 0
    num_iters = len(publdata)
    if sample_size > 0 and sample_size < num_iters : num_iters = sample_size
    for i, row in publdata.iterrows() :
        if i >= num_iters : break
        print("\r{:2.0f}% done...".format(min(99,100*i/num_iters)),end='')
        
        item_idx, year, doi, journal, mpg = row
        #print("\n--- {}, {}, {}, {}, {}\n".format(item_idx,year,doi, journal, mpg)) # for testing
        
        try:
            item_id = id_dict[item_idx]
        except: 
            num_missing_idx += 1       
            #item_id = '1000000001' # for testing
            continue

        # user # has separator so replace if present
        if not isinstance(journal,str) : 
            journal = ""
        else:
            journal = journal.replace('#','')
        if not isinstance(doi,str) : 
            doi = ""
        else:
            doi     = doi.replace('#','')

        publstr = "{}#{}".format(journal,doi)
        max_str_len = max(max_str_len,len(publstr))

        cmd = 'UPDATE {} SET publ = %s, nummpg = %s WHERE id = %s'.format(meta_table)
        arg = (publstr,str(mpg),str(item_id))
        if not mysql.execute_mutating_query(db_cursor, dry_run, cmd, arg):
            return
        num_ids_updated += 1
    print('\n')
    
    print("{} papers updated".format(num_ids_updated))
    print("{} missing item_idx".format(num_missing_idx))
    print("Maximum publication string length = {}".format(max_str_len))


def add_abstracts_to_abstract_table(db_cursor, dry_run, abstract_table, abstracts_file, idmap_file, new_table=True, sample_size=-1):

    """
    Fill abstract table with abstracts
    """

    if new_table and not dry_run :
        create_new_abstract_table(db_cursor, abstract_table)

    print("Reading in {}".format(idmap_file))
    idmap = pd.read_csv(idmap_file,sep='\t',header=0,names=('item_id','item_idx'),quoting=3)
    id_dict = dict(zip(idmap["item_idx"],idmap["item_id"]))
    del idmap

    print("Reading in {}".format(abstracts_file))
    abstracts = pd.read_csv(abstracts_file,sep='\t',header=0,names=('item_idx','num_paragraph','abstract'),usecols=('item_idx','num_paragraph','abstract'),quoting=3)
    abstracts.sort_values('item_idx',inplace=True)
    abstracts.index = range(0,len(abstracts))

    print("Adding abstracts to {}".format(abstract_table))
    num_ids_updated = 0
    num_missing_idx = 0
    max_str_len     = 0
    max_num_para    = 0
    curr_idx   = -1
    paragraphs = []
    num_iters = len(abstracts)
    if sample_size > 0 and sample_size < num_iters : num_iters = sample_size
    for i, row in abstracts.iterrows() :
        if i >= num_iters : break
        print("\r{:2.0f}% done...".format(min(99,100*i/num_iters)),end='')
        
        item_idx, num_paragraph, abstract = row
        #print("\n--- {}, {}, {}\n".format(item_idx,num_paragraph, len(abstract))) # for testing
        
        if i > 0 and item_idx != curr_idx :

            try:
                item_id = id_dict[item_idx]
            except: 
                num_missing_idx += 1       
                continue
            
            max_num_para = max(max_num_para,len(paragraphs))
            # make sure paragraphs are sorted
            paragraphs.sort(key=lambda x : x[1])
            complete_abstract = ''.join([txt for txt,_ in paragraphs])
            
            max_str_len = max(max_str_len,len(complete_abstract))

            # arrived at new id, so record old one
            cmd = 'INSERT INTO {} (id,abstract) VALUES (%s,%s)'.format(abstract_table)
            arg = (str(item_id),complete_abstract)
            if not mysql.execute_mutating_query(db_cursor, dry_run, cmd, arg):
                return
            num_ids_updated += 1
            
            paragraphs = []
        
        paragraphs.append([abstract,num_paragraph])
        curr_idx = item_idx
    print('\n')

    print("{} max abstract string length".format(max_str_len))
    print("{} is max number paragraphs".format(max_num_para))
    print("{} papers updated".format(num_ids_updated))
    print("{} missing item_idx".format(num_missing_idx))


def add_empty_papers_to_cite_table (db_cursor, dry_run, meta_table, cite_table, new_table=False):
    """
    Add remaining papers from meta_data to cite table i.e. those with no references
    """

    if new_table and not dry_run :
        create_new_cite_table(db_cursor,cite_table)

    ids = []
    hits = db_cursor.execute('SELECT {0}.id FROM {0} LEFT OUTER JOIN {1} ON ({0}.id = {1}.id) WHERE {1}.id IS NULL'.format(meta_table,cite_table))
    for _ in range(hits) :
        ids.append(db_cursor.fetchone()[0])


    for id in ids :
        cmd = 'INSERT INTO {} (id) VALUES (%s)'.format(cite_table)
        arg = (id)
        if not mysql.execute_mutating_query(db_cursor, dry_run, cmd, arg):
            return


def assign_main_categories_to_meta_table(db_cursor, dry_run, meta_table, cite_table, sample_size=-1) :
    """
    Assign main categories for papers

    If a paper has multiple categories, tally which of these categories occurs
    most frequently among its references

    Also sort category list and reinserts in order of most frequent
    """

    # First assign maincats for papers with only a single category
    print("Assigning maincats for papers with only a single category")
    cmd = 'UPDATE {} SET maincat = catcodes WHERE numcat = 1'.format(meta_table)
    arg = ()
    if not mysql.execute_mutating_query(db_cursor, dry_run, cmd, arg):
        return
    
    # Second assign maincats for papers with more than one category
    
    ids = {}
    if sample_size > 0:
        hits = db_cursor.execute('SELECT id,catcodes FROM {} WHERE numcat > 1 LIMIT {}'.format(meta_table,sample_size))
    else:
        hits = db_cursor.execute('SELECT id,catcodes FROM {} WHERE numcat > 1'.format(meta_table))
    print("Assigning maincats for {} papers with multiple categories".format(hits))
    for _ in range(hits) :
        id, catcodes = db_cursor.fetchone()
        ids[id] = catcodes.split(',')


    num_no_refs           = 0 # number of papers encountered with no references
    num_no_matching_cats  = 0 # number of papers where references have no cas in common
    num_ambig_cats        = 0 # number of papers where no winning category
    counter_i = 0
    counter_tot = len(ids)
    for id, cats in ids.items() :
        print("\r{:2.0f}% done...".format(min(99,100*(counter_i)/counter_tot)),end='')
        counter_i += 1

        if db_cursor.execute('SELECT numRefs,refs FROM {} WHERE id = {}'.format(cite_table,id)) == 1 :
            num_refs, refs = db_cursor.fetchone()
            refs_list = decodeBlob(refs)
        else :
            print("\nWarning: could not lookup refs for paper {}".format(id))
            continue

        # determine maincat
    
        if num_refs == 0 :
            # pick first category (what more can we do?)
            maincat = cats[0]
            num_no_refs += 1
        else :

            cats_dict = {}
            for cat in cats :
                cats_dict[cat] = 0

            for ref_id, _ in refs_list :

                # Get categories for each reference
                if db_cursor.execute('SELECT numcat,catcodes FROM {} WHERE id = {}'.format(meta_table,ref_id)) == 1 :
                    num_ref_cats, ref_cats = db_cursor.fetchone()
                    if num_ref_cats > 0 :
                        ref_cats = ref_cats.split(',')
                    else : continue
                else :
                    print("\nWarning: could not lookup categories for ref {} of paper {}".format(ref_id,id))
                    continue
                
                for ref_cat in ref_cats :
                    try :
                        cats_dict[ref_cat] += 1
                        #cats_dict[ref_cat] += 1/len(ref_cats)
                    except : None
            
            sorted_cats_dict = sorted(cats_dict.items(), key=lambda x: x[1], reverse=True)
            sorted_cats = [cat for cat,freq in sorted_cats_dict]
            maincat = sorted_cats[0]
            if sorted_cats_dict[0][1] == 0 :
                num_no_matching_cats += 1
            elif sorted_cats_dict[0][1] == sorted_cats_dict[1][1] :
                num_ambig_cats += 1
    
        cmd = 'UPDATE {} SET maincat = %s, catcodes = %s WHERE id = %s'.format(meta_table)
        arg = (maincat,",".join(sorted_cats),id)
        if not mysql.execute_mutating_query(db_cursor, dry_run, cmd, arg):
            return
    print('\n')

    print("Number of maincat choices with no refs: {} ({:3.1f}%)".format(num_no_refs,100*num_no_refs/len(ids)))
    print("Number of ambigious maincat choices:    {} ({:3.1f}%)".format(num_ambig_cats,100*num_ambig_cats/len(ids)))
    print("Number of zero freq. maincat choices:   {} ({:3.1f}%)".format(num_no_matching_cats,100*num_no_matching_cats/len(ids)))

def assign_main_big_categories_to_meta_table(db_cursor, dry_run, meta_table, bigcat_file, sample_size=-1) :
    """
    Assign main big categories for papers

    Simply use previously computed maincat for assignment
    """

    bigcats = pd.read_csv(bigcat_file,delim_whitespace=True,header=None,names=('cat','bigcat'))

    print("Adding main big categories to meta_table")
    
    # make dictionary
    cats_dict = {}
    for i, row in bigcats.iterrows() :
        cat, bigcat = row
        cats_dict[cat.strip().lower()] = bigcat.strip().lower()

    print("Getting WOS categories from DB and finding main big cats")
    ids = {}
    missing_maincats = {}
    if sample_size > 0:
        hits = db_cursor.execute('SELECT id,maincat FROM {} LIMIT {}'.format(meta_table,sample_size))
    else:
        hits = db_cursor.execute('SELECT id,maincat FROM {}'.format(meta_table))
    for i in range(hits) :
        print("\r{:2.0f}% done...".format(min(99,100*(i)/hits)),end='')
        id, maincat = db_cursor.fetchone()
        
        if maincat is not None and maincat > "" :
            try : 
                ids[id] = cats_dict[maincat]
            except : 
                missing_maincats[maincat] = True

    print('\n')
    print("Missing cats: ",missing_maincats)

    print("Inserting main bigcats back into DB")
    i = 0
    for id,bigcat in ids.items() :
        print("\r{:2.0f}% done...".format(min(99,100*(i)/len(ids))),end='')
        i += 1
        cmd = 'UPDATE {} SET mainbigcat = %s WHERE id = %s'.format(meta_table)
        arg = (bigcat,id)
        if not mysql.execute_mutating_query(db_cursor, dry_run, cmd, arg):
            return
            
    print('\n')

def assign_ref_kw_freq_to_cite_table(db_cursor, dry_run, meta_table, cite_table, sample_size=-1):
    """
    Count matching keywords between parent and child of each reference link, and assign
    this to 'reference frequency' field in cite table blobs

    Simultaneously sort keywords on their overall frequency among references

    NOTE 1: this will increase size of ref blob size from 6 to 8 bytes
    NOTE 2: need to re-run refresh-cites

    TODO:
    - Not currently working because:
        - Need to update reference blobs of non-selected fields too
        - Once updated, refs blobs become incompatible with remaining methods in this file
    """

    ids = {}
    query = "SELECT {0:}.id,{1:}.keywords FROM {0:},{1:} WHERE ({0:}.id = {1:}.id AND {1:}.keywords IS NOT NULL AND {0:}.numRefs > 0)".format(cite_table,meta_table)
    if sample_size > 0:
        hits = db_cursor.execute(query + " LIMIT {}".format(sample_size))
    else:
        hits = db_cursor.execute(query)
    print("Assigning keyword ref freq for {} papers".format(hits))
    for _ in range(hits) :
        id,keywords = db_cursor.fetchone()
        kws = keywords.split(',')
        assert(len(kws) != 0)
        ids[id] = set(kws)

    counter_i = 0
    counter_tot = len(ids)
    sum_kw_matches = 0
    sum_references = 0
    max_num_matching = 0
    max_num_matching_id = 0
    nonzero_kw_matches = 0
    for parent_id, parent_kws in ids.items() :
        print("\r{:2.0f}% done...".format(min(99,100*(counter_i)/counter_tot)),end='')
        counter_i += 1

        if db_cursor.execute('SELECT numRefs,refs FROM {} WHERE id = {}'.format(cite_table,parent_id)) == 1 :
            num_refs, refs = db_cursor.fetchone()
            #if (num_refs*8) % 6 == 0: continue # TMP FIXME
            #if len(refs) % 6 != 0: continue # TMP FIXME
            refs_list = decodeBlob(refs)
        else :
            print("\nWarning: could not lookup refs for paper {}".format(parent_id))
            continue

        # create dict counter for parent kws
        # TODO make this prettier
        kws_counter = {}
        for kw in list(parent_kws):
            slimkw = kw.replace(' ','').replace('-','')
            kws_counter[slimkw] = [kw,0]
        slimkw_set = set([key for key,_ in kws_counter.items()])

        new_refs_list = []
        for ref in refs_list :
            child_id = ref[0]
            no_child_kws = False
            # keywords for parent and child
            if db_cursor.execute('SELECT keywords FROM {} WHERE id = {}'.format(meta_table,child_id)) == 1 :
                child_kws = db_cursor.fetchone()[0]
            else:
                print("\nWarning: could not lookup keywords for reference {} of paper {}".format(child_id,parent_id))
                no_child_kws = True
        
            if not no_child_kws and child_kws is None or child_kws == "":
                no_child_kws = True
           
            num_matching = 0
            if not no_child_kws:
                # TODO make this prettier
                child_kws = child_kws.replace(' ','').replace('-','').split(',')
                #child_kws = child_kws.split(',')
                #inter_kws    = parent_kws.intersection(child_kws)
                inter_kws    = slimkw_set.intersection(child_kws)
                num_matching = len(inter_kws)
                
                if num_matching > 0:
                    #print("\nmatching: {}, for parent: {}, child: {}\n".format(num_matching,";".join(parent_kws),";".join(child_kws)))
                    sum_kw_matches += num_matching
                    nonzero_kw_matches += 1
                    if len(parent_kws) > 1:
                        for kw in inter_kws:
                            kws_counter[kw][1] += 1
                    if num_matching > max_num_matching:
                        max_num_matching = num_matching
                        max_num_matching_id = parent_id

            # use 1+num_matching as 'reference frequency'
            new_refs_list.append((child_id,1+num_matching,0))
        
        # update cite table
        new_refs_blob = buildBlob(new_refs_list,True)
        assert(len(new_refs_blob)/8 == num_refs)
        sum_references += num_refs
        cmd = 'UPDATE {} SET refs = %s WHERE id = %s'.format(cite_table)
        arg = (bytes(new_refs_blob),str(parent_id))
        if not mysql.execute_mutating_query(db_cursor, dry_run, cmd, arg):
            return

        # update meta table
        if len(parent_kws) > 1 and sum([val[1] for _,val in kws_counter.items()]) > 0 :
            new_parent_kws = [val[0] for _,val in sorted(kws_counter.items(), key=lambda x: x[1][1], reverse=True)]
            cmd = 'UPDATE {} SET keywords = %s WHERE id = %s'.format(meta_table)
            arg = (','.join(new_parent_kws),str(parent_id))
            if not mysql.execute_mutating_query(db_cursor, dry_run, cmd, arg):
                return

    print('\n')
    
    print("{} max number keywords matching for id {}".format(max_num_matching,max_num_matching_id))
    print("{} papers with non-trivial keywords, with {:.2f} refs on avg".format(len(ids),sum_references/len(ids)))
    print("{:.2f} avg keyword matches per reference".format(sum_kw_matches/sum_references))
    print("{:.2f} keyword matches per paper".format(sum_kw_matches/len(ids)))
    print("{:.2f}  = proportion references with one or matches".format(nonzero_kw_matches/sum_references))
    #print("{} average references with one or matches per paper".format(nonzero_kw_matches/len(ids)))
    print("{:.2f} avg keyword matches for references with non-zero match".format(sum_kw_matches/nonzero_kw_matches))



if __name__ == "__main__":

    cmd_parser = argparse.ArgumentParser(description='convert MPDL csv data into database tables.')
    cmd_parser.add_argument('--db', metavar='<MySQL database>', help='server name (or localhost) of MySQL database to connect to')
    cmd_parser.add_argument('--dry-run', action='store_true', help='do not do anything destructive (like modify the database)')
    cmd_parser.add_argument('--meta-table', metavar='<meta table>', default='meta_data', help='name of meta data table')
    cmd_parser.add_argument('--new-meta-table', action='store_true', help='create new meta table')
    cmd_parser.add_argument('--cite-table', metavar='<citation table>', default='pcite', help='name of citation table')
    cmd_parser.add_argument('--new-cite-table', action='store_true', help='create new citation table')
    cmd_parser.add_argument('--abstract-table', metavar='<abstract table>', default='abstracts', help='name of abstracts table')
    cmd_parser.add_argument('--new-abstract-table', action='store_true', help='create new abstract table')
    cmd_parser.add_argument('--add-items', action='store_true', help='populate meta table with items data')
    cmd_parser.add_argument('--add-cats', action='store_true', help='add subject category data to meta table')
    cmd_parser.add_argument('--add-refs', action='store_true', help='add reference data to citation table')
    cmd_parser.add_argument('--add-titles', action='store_true', help='populate meta table with title data')
    cmd_parser.add_argument('--add-authors', action='store_true', help='populate meta table with authors data')
    cmd_parser.add_argument('--add-keywords', action='store_true', help='populate meta table with keyword data')
    cmd_parser.add_argument('--add-publdata', action='store_true', help='populate meta table with publication data')
    cmd_parser.add_argument('--add-abstracts', action='store_true', help='populate abstract table with abstracts')
    cmd_parser.add_argument('--fill-cite-table', action='store_true', help='complete citation table with missing items from meta table')
    cmd_parser.add_argument('--add-big-cats', action='store_true', help='add big superset categories to meta table')
    cmd_parser.add_argument('--assign-main-cats', action='store_true', help='assign main categories to meta table')
    cmd_parser.add_argument('--assign-main-big-cats', action='store_true', help='assign main big categories to meta table')
    cmd_parser.add_argument('--assign-ref-kw-freq', action='store_true', help='assign reference frequency based on keywords in common')
    cmd_parser.add_argument('--sample', metavar='<sample size>', help='sample size to process for testing (-1 means all)',type=int,default=-1)
    args = cmd_parser.parse_args()

    time_start = datetime.datetime.now()
    print('[csv_to_db] started processing at', str(time_start))

    db_connection = mysql.dbconnect(args.db, verbose=0)
    db_cursor = db_connection.cursor()

    if args.new_meta_table and not args.dry_run :
        create_new_meta_table(db_cursor, args.meta_table)

    if args.new_cite_table and not args.dry_run :
        create_new_cite_table(db_cursor, args.cite_table)

    if args.new_abstract_table and not args.dry_run :
        create_new_abstract_table(db_cursor, args.abstract_table)

    items_file     = "local-data/sub_wos_paperscape_item.csv" 
    subjects_file  = "local-data/sub_wos_paperscape_item_x_subj.csv"
    xref_file      = "local-data/sub_wos_paperscape_item_x_xref.csv"
    bigcat_file    = "local-data/oecd_cats.csv"

    # new meta data (fields are tab separated)
    idmap_file     = "local-data/swox_schem_map_old2new.txt"
    titles_file    = "local-data/swox_schem_201609/swox_schem_item_u_ti.txt"
    authors_file   = "local-data/swox_schem_201609/swox_schem_item_x_itau.txt"
    keywords_file  = "local-data/swox_schem_201609/swox_schem_item_x_itkw.txt"
    abstracts_file = "local-data/swox_schem_201609/swox_schem_item_x_itab.txt"
    publdata_file  = "local-data/swox_schem_201609/swox_schem_item.txt"

    if args.add_items :
        populate_meta_table(db_cursor, args.dry_run, args.meta_table, items_file,sample_size=args.sample)

    if args.add_cats :
        add_categories_to_meta_table(db_cursor, args.dry_run, args.meta_table, subjects_file,sample_size=args.sample)

    if args.add_refs :
        add_refs_to_cite_table(db_cursor,args.dry_run,args.cite_table,xref_file,meta_table=args.meta_table,sample_size=args.sample)

    if args.add_titles :
        add_titles_to_meta_table(db_cursor, args.dry_run, args.meta_table, titles_file, idmap_file, sample_size=args.sample)

    if args.add_authors :
        add_authors_to_meta_table(db_cursor, args.dry_run, args.meta_table, authors_file, idmap_file, sample_size=args.sample)

    if args.add_keywords :
        add_keywords_to_meta_table(db_cursor, args.dry_run, args.meta_table, keywords_file, idmap_file, sample_size=args.sample)

    if args.add_publdata :
        add_publdata_to_meta_table(db_cursor, args.dry_run, args.meta_table, publdata_file, idmap_file, sample_size=args.sample)
    
    if args.add_abstracts :
        add_abstracts_to_abstract_table(db_cursor, args.dry_run, args.abstract_table, abstracts_file, idmap_file, sample_size=args.sample)

    if args.fill_cite_table :
        add_empty_papers_to_cite_table(db_cursor,args.dry_run,args.meta_table,args.cite_table)

    if args.add_big_cats :
        add_big_categories_to_meta_table(db_cursor, args.dry_run, args.meta_table, bigcat_file)

    if args.assign_main_cats :
        assign_main_categories_to_meta_table(db_cursor, args.dry_run, args.meta_table, args.cite_table, sample_size=args.sample)

    if args.assign_main_big_cats :
        assign_main_big_categories_to_meta_table(db_cursor, args.dry_run, args.meta_table, bigcat_file, sample_size=args.sample)

    if args.assign_ref_kw_freq :
        assign_ref_kw_freq_to_cite_table(db_cursor, args.dry_run, args.meta_table, args.cite_table, sample_size=args.sample)

    db_connection.close()

    time_end = datetime.datetime.now()
    print('[csv_to_db] finished processing at', str(time_end))
