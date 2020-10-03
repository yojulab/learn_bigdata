# https://hdfscli.readthedocs.io/en/latest/quickstart.html
from hdfs import InsecureClient
client = InsecureClient(url='http://localhost:9870/', user='root')

# from hdfs import Config
# client = Config().get_client('dev')

# Retrieving a file or folder content summary.
content = client.content('/test_datas')

# Listing all files inside a directory.
fnames = client.list('/test_datas')

# Retrieving a file or folder status.
# status = client.status('/test_datas/sample1.txt')

# Renaming ("moving") a file.
# client.rename('/test_datas/sample1.txt', '/test_datas/sample5.txt')

# Deleting a file or folder.
# client.delete('/test_temps', recursive=True)

hdfspath = '/test_datas/'
localpath = '/Users/janevallette/Documents/Develops/learn_bigdata/datas/cat.jpeg'
result = client.upload(hdfspath, localpath)

# Writing part of a file.
# with open('datas/upfile.txt') as reader, client.write('/test_datas/upfile1.txt') as writer:
#   for line in reader:
#     # if line.startswith('-'):
#       writer.write(line)

from json import dump
# Writing a serialized JSON object.
# with open('datas/model.json') as reader, client.write('/test_datas/model1.json') as writer:
#   dump(reader, writer)

# with open('datas/cat.jpeg') as reader, client.write('/test_files/cat1.jpeg') as writer:
#   dump(reader, writer)

# Download a file or folder locally.
client.download('/test_datas/', 'datas/', n_threads=5)

# Loading a file in memory.
# with client.read('/test_datas/sample2.txt') as reader:
#   sample1 = reader.read()

# # Directly deserializing a JSON object.
# with client.read('/test_datas/model1.json', encoding='utf-8') as reader:
#   from json import load
#   model = load(reader)