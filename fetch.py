import io
import argparse
import ir_datasets
import pkgutil
import json
import sys
import datetime

parser = argparse.ArgumentParser()
parser.add_argument('download_id')
parser.add_argument('--list', action='store_true')
args = parser.parse_args()

downloads = json.loads(pkgutil.get_data('ir_datasets', 'etc/downloads.json'))

def iter_downloads(data, prefix=''):
  if 'url' in data and 'expected_md5' in data:
    yield prefix, data
  elif 'instructions' in data:
    pass
  else:
    for key in data.keys():
      yield from iter_downloads(data[key], prefix=f'{prefix}/{key}' if prefix else key)

download = None
for dlid, dlc in iter_downloads(downloads):
  if args.list:
    print(f'{dlid}\t{dlc["url"]}\t{dlc["expected_md5"]}')
  if dlid == args.download_id:
    download = dlc
    if not args.list:
      break

if not download:
  print(f'could not find download {args.download_id}')
  sys.exit(-1)

dlc = ir_datasets.util.Download([ir_datasets.util.RequestsDownload(download['url'])], expected_md5=download['expected_md5'], stream=True)
with ir_datasets.util.finialized_file(download['expected_md5'], 'wb') as fout:
  with dlc.stream() as stream:
    inp = stream.read(io.DEFAULT_BUFFER_SIZE)
    while len(inp) > 0:
      fout.write(inp)
      inp = stream.read(io.DEFAULT_BUFFER_SIZE)

with open('README.md', 'at') as f:
  f.write(f' - `{download["expected_md5"]}`: {download["url"]} @ {datetime.datetime.now().isoformat()}\n')
