lucene-join-solr-query-parser
=============================

* load the query parser drop-in jar https://wiki.apache.org/solr/SolrPlugins#How_to_Load_Plugins

* enable the query parser by adding into solrconfig.xml
 
\<queryParser name="scorejoin" class="join.ScoreJoinQParserPlugin"/\>

* call it almost like standard join query parser http://wiki.apache.org/solr/Join but you also can specify 'score' parameter 

q={!scorejoin from=prod_id to=id score=max}title:ipod

* available score parameter values are: None, Avg, Max, Total. Lowercased valued should also work, follow http://lucene.apache.org/core/4_6_0/join/org/apache/lucene/search/join/ScoreMode.html for the description.
