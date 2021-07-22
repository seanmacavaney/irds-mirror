import os
import json
import pkgutil

downloads = json.loads(pkgutil.get_data('ir_datasets', 'etc/downloads.json'))

def iter_downloads(data, prefix=''):
  if 'url' in data and 'expected_md5' in data:
    yield prefix, data
  elif 'instructions' in data:
    pass
  else:
    for key in data.keys():
      yield from iter_downloads(data[key], prefix=f'{prefix}/{key}' if prefix else key)

for name, data in iter_downloads(downloads):
  exists = 'IN_CACHE' if os.path.exists(data['expected_md5']) else 'NOT_IN_CACHE'
  print(name, data.get('size_hint'), exists, data['url'])
