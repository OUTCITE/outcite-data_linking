index=$1;

echo $index;
for target in sowiport crossref dnb openalex; do
    echo $target;
    #for field in anystyle_references_from_cermine_fulltext anystyle_references_from_cermine_refstrings anystyle_references_from_grobid_fulltext anystyle_references_from_grobid_refstrings anystyle_references_from_gold_fulltext anystyle_references_from_gold_refstrings cermine_references_from_cermine_xml cermine_references_from_grobid_refstrings cermine_references_from_gold_refstrings grobid_references_from_grobid_xml; do
    #    echo $field;
    #    python code/add_field.py $index ${field}.${target}_url none overwrite;
    #done
    python code/add_field.py $index has_${target}_urls false overwrite;
done
