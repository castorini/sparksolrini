#!/bin/bash

target/appassembler/bin/IndexCollection -collection NewYorkTimesCollection -generator JsoupGenerator \
  -storePositions -storeDocvectors -storeRawDocs -memorybuffer 256 \
  -threads 24 -input /hdd2/collections/NYTcorpus -index lucene-index.core17.pos+docvectors >& log.core17.pos+docvectors+rawdocs
