import argparse
import datetime

import requests


# Perform a search in Solr
def search(solr, index, query):
    return requests.get("%s/solr/%s/select?q=%s" % (solr, index, query)).json()


# Update a document in Solr
def update(solr, index, data):
    return requests.post('%s/solr/%s/update?commit=true' % (solr, index), json=data)


# Print the docs
def print_docs(response):
    for doc in response['response']['docs']:
        print(doc)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--solr", default="http://localhost:8983", type=str, help="The URL of the Solr instance")
    parser.add_argument("--index", required=True, type=str, help="The Solr index to use")
    parser.add_argument("--query", required=True, type=str, help="The search query to run")

    # Parse the command line args
    args = parser.parse_args()

    """ Do a search """

    print("Searching...")

    # Do a search
    search_resp = search(args.solr, args.index, args.query)
    print_docs(search_resp)

    """ Do an update """

    print("\nUpdating...")

    # Document to update
    id = 1425397
    data = [{'id': id, 'contents': "We updated the doc. from Python @ %s" % str(datetime.datetime.now())}]

    print("Before update:")

    # Print the doc to show it was updated
    search_resp = search(args.solr, args.index, "id:" + str(id))
    print_docs(search_resp)

    # Run the update
    update(args.solr, args.index, data)

    print("After update:")

    # Print the doc to show it was updated
    search_resp = search(args.solr, args.index, "id:" + str(id))
    print_docs(search_resp)
