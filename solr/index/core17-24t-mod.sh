#!/bin/bash

target/appassembler/bin/IndexCollection -collection NewYorkTimesCollection -generator JsoupGenerator \
      -storePositions -storeDocvectors -storeRawDocs \
      -threads 24 -input /hdd2/collections/NYTcorpus -index lucene-index.core17.pos+docvectors.0 -mod 0 >& log.core17.pos+docvectors+rawdocs.0

target/appassembler/bin/IndexCollection -collection NewYorkTimesCollection -generator JsoupGenerator \
      -storePositions -storeDocvectors -storeRawDocs \
      -threads 24 -input /hdd2/collections/NYTcorpus -index lucene-index.core17.pos+docvectors.1 -mod 1 >& log.core17.pos+docvectors+rawdocs.1

target/appassembler/bin/IndexCollection -collection NewYorkTimesCollection -generator JsoupGenerator \
      -storePositions -storeDocvectors -storeRawDocs \
      -threads 24 -input /hdd2/collections/NYTcorpus -index lucene-index.core17.pos+docvectors.2 -mod 2 >& log.core17.pos+docvectors+rawdocs.2

target/appassembler/bin/IndexCollection -collection NewYorkTimesCollection -generator JsoupGenerator \
      -storePositions -storeDocvectors -storeRawDocs \
      -threads 24 -input /hdd2/collections/NYTcorpus -index lucene-index.core17.pos+docvectors.3 -mod 3 >& log.core17.pos+docvectors+rawdocs.3

target/appassembler/bin/IndexCollection -collection NewYorkTimesCollection -generator JsoupGenerator \
      -storePositions -storeDocvectors -storeRawDocs \
      -threads 24 -input /hdd2/collections/NYTcorpus -index lucene-index.core17.pos+docvectors.4 -mod 4 >& log.core17.pos+docvectors+rawdocs.4
