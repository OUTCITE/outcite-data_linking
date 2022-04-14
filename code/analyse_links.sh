database=$1

for pipeline in anystyle_references_from_cermine_fulltext anystyle_references_from_cermine_refstrings anystyle_references_from_grobid_refstrings cermine_references_from_cermine_xml cermine_references_from_grobid_refstrings grobid_references_from_grobid_xml; do
    num_refer=`sqlite3 $database "select count(*) from metadata_${pipeline}"`;
    echo ----------------------------------------------------------------------
    echo $pipeline: $num_refer references;
    echo ----------------------------------------------------------------------
    for target in sowiport crossref dnb openalex; do
        num_match=`sqlite3 $database "select count(*) from links_${pipeline} where toID_${target} is not null"`;
        num_nourl=`sqlite3 $database "select count(*) from links_${pipeline} where toID_${target} is not null and toURL_${target} is null"`;
        echo ----- $target: $num_match matches with $num_nourl missing URLs
        if [[ $num_refer != 0 ]]; then
            echo `expr $num_match \* 100 / $num_refer`"% of references in ${pipeline} have a ${target} ID";
        else
            echo "0% of references in ${pipeline} have a ${target} ID";
        fi
        if [[ $num_match != 0 ]]; then
            echo `expr $num_nourl \* 100 / $num_match`"% of ${target} IDs have no URL in ${pipeline}";
        else
            echo "0% of ${target} IDs have no URL in ${pipeline}";
        fi
    done;
    echo `sqlite3 $database "select refstr from metadata_${pipeline} where linkID not in ( select distinct linkID from links_${pipeline} )"` > unmatched_${pipeline};
done

# The below allows to have a look at the additional references from grobid that seem not to be matched to a large degree <-- But these will be found in any other one match collection!
#select title from metadata_anystyle_references_from_grobid_refstrings where linkID in (
#    select distinct linkID from links_anystyle_references_from_grobid_refstrings where fromID not in (
#        select distinct fromID from links_anystyle_references_from_cermine_fulltext
#    ) limit 0,10
#);

# The below is similar to the above but uses only references unmatched in sowiport, where a larger difference in percent matched was observed <-- But these will be found in any other one match collection!
#select title from metadata_anystyle_references_from_grobid_refstrings where linkID in (
#    select distinct linkID from links_anystyle_references_from_grobid_refstrings where fromID not in (
#        select distinct fromID from links_anystyle_references_from_cermine_refstrings
#    ) and toID_sowiport is null limit 10,10
#);

# The below is the true title information where no match could be made for anystyle_references_from_grobid_refstrings
#echo `select title from metadata_anystyle_references_from_grobid_refstrings where linkID not in (
#    select distinct linkID from links_anystyle_references_from_grobid_refstrings
#)` > 

# The below is the true title information where no match could be made for anystyle_references_from_cermine_fulltext
#select title from metadata_anystyle_references_from_cermine_fulltext where linkID not in (
#    select distinct linkID from links_anystyle_references_from_cermine_fulltext
#) limit 0,10;
