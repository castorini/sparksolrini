#!/bin/bash

target/appassembler/bin/IndexCollection -collection NewYorkTimesCollection -generator JsoupGenerator \
  -storePositions -storeDocvectors -storeRawDocs -solr -solr.url http://192.168.152.201:30852/solr/core17 \
  -threads 24 -input /hdd2/collections/NYTcorpus/data -index lucene-index.core17.pos+docvectors >& log.core17.pos+docvectors+rawdocs
