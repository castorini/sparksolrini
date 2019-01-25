### Tembo experiment result

|          | size        |  SolrPartition   | ThreadedSolr (ms) | Solr Query (ms) | SparkSolr (ms) |  SparkSolr Query (ms) | SparkHDFS (ms) |
|----------|-------------|-----------------|-----------|------------|-------------------|----------------|----------------|
| napoleon | 10,522     |      10         | 59,345   |    35,460    |      23,196      |         16,216   |     2,254,306    |
| interpol | 50,551     |      10           | 191,769  |   135,015   |     152,145      |      105,891   |     2,256,404    |
| belt     | 99,287     |      10             | 312,432  |  241,726  |     182,208      |    169,849   |     2,286,729    |
| kind     | 506,423     |     50           | 1,000,119 |   910,737  |     625,148      |      522,089   |     2,296,842    |
| idea     | 1,003,549    |     100         | 1,625,906  |  1,496,897  |    1,720,868      |   1,491,187   |     2,402,400    |
| current  | 4,897,864   |      485         | 3,950,407 |  3,864,894   |    9,797,357      |   8,425,061   |     2,344,478    |
| other    | 9,988,438   |      995         | 6,342,624 |   6,193,269  |   19,737,940      |  16,654,290   |     2,494,579    |
| public   | 15,568,449  |     1555         | 9,462,147 |   9,196,083  |   28,968,021      |  27,016,566   |     2,513,627    |

### Tweet time zone results (mb11)
```
earthquake - top 10 time zones with the most tweets
	Pacific Time (US & Canada) --> 250
	Jakarta --> 196
	Eastern Time (US & Canada) --> 136
	Central Time (US & Canada) --> 101
	Quito --> 82
	Tokyo --> 64
	Arizona --> 58
	Alaska --> 42
	Santiago --> 39
	London --> 38
snowstorm - top 10 time zones with the most tweets
	Eastern Time (US & Canada) --> 288
	Central Time (US & Canada) --> 189
	Quito --> 77
	Pacific Time (US & Canada) --> 43
	Mountain Time (US & Canada) --> 26
	Atlantic Time (Canada) --> 10
	Alaska --> 9
	London --> 7
	Arizona --> 5
	Amsterdam --> 5
```
