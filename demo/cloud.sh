#!/bin/bash

wordcloud_cli --text /tmp/ner/part-00000 --imagefile ner.png --width 500 --height 500 --no_collocations --background "#FFFFFF" --color "#C70039" --regexp "(.*)\n?"
