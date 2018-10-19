import datetime

import requests


def read(id):
    """
        Read a document from Solr with a given ID.
    """

    response = requests.get('http://localhost:8983/solr/core17/select?q=id:%s' % id)

    data = response.json()

    for doc in data['response']['docs']:
        print(doc)


def write(data):
    """
    Update a document in Solr.
    """
    response = requests.post('http://localhost:8983/solr/core17/update?commit=true', json=data)
    print(response.json())


if __name__ == '__main__':
    id = 1425397
    data = [{'id': id, 'contents': "We updated the doc. from Python @ %s" % str(datetime.datetime.now())}]

    write(data)
    read(id)
