
import multiprocessing as mp
from multiprocessing import cpu_count

import pandas as pd
import requests
import numpy as np
from bs4 import BeautifulSoup
from urllib.request import urlopen
from gutenberg.acquire import load_etext
from gutenberg.cleanup import strip_headers
import sys

# only removes funny tokens for English texts
def remove_funny_tokens(text):
    tokens = text.split()
    sample = ' '.join(' '.join(tokens).replace('xe2x80x9c', ' ').replace('xe2x80x9d', ' ')\
                                      .replace('xe2x80x94', ' ').replace('xe2x80x99', "'")\
                                      .replace('xe2x80x98', "'").split())
    return sample

# clean newlines, carriage returns and tabs
def clean_text(text):
    cleaned_listed_text = []
    listed_text = list(text)

    for iter in range(len(listed_text) - 1):
        if (listed_text[iter] == '\\' and listed_text[iter + 1] == 'n') or \
            (listed_text[iter] == 'n' and listed_text[iter - 1] == '\\'):
            continue
        elif listed_text[iter] == '\\' and listed_text[iter + 1] == 'r' or \
            (listed_text[iter] == 'r' and listed_text[iter - 1] == '\\'):
            continue
        elif listed_text[iter] == '\\' and listed_text[iter + 1] == 't' or \
            (listed_text[iter] == 't' and listed_text[iter - 1] == '\\'):
            continue
        elif listed_text[iter] == '\\':
            continue
        else:
            cleaned_listed_text.append(listed_text[iter])

    cleaned_text = ''.join([str(char) for char in cleaned_listed_text])
    cleaned_text = remove_funny_tokens(cleaned_text)

    return ''.join(cleaned_text)

def load_gutenburg_shard(output_name, shard_num, no_shards):

  # I don't like this but it avoids copying data when intilizing the process
  df_metadata = pd.read_csv('gutenberg_metadata.csv')
  metadata_shards = np.array_split(df_metadata, no_shards)
  metadata = metadata_shards[shard_num]

  data = {'Author': None, 'Title': None, 'Link': None, 'ID': None, 'Bookshelf': None, 'Text': None}

  for key, row in metadata.iterrows():

      if data['Author'] == None:
          data['Author'] = [row['Author']]
      else:
          data['Author'].append(row['Author'])

      if data['Title'] == None:
          data['Title'] = [row['Title']]
      else:
          data['Title'].append(row['Title'])

      if data['Link'] == None:
          data['Link'] = [row['Link']]
      else:
          data['Link'].append(row['Link'])

      book_id = int(row['Link'].split('/')[-1])

      if data['ID'] == None:
          data['ID'] = [book_id]
      else:
          data['ID'].append(book_id)

      if data['Bookshelf'] == None:
          data['Bookshelf'] = [row['Bookshelf']]
      else:
          data['Bookshelf'].append(row['Bookshelf'])

      text = np.nan
      try:
          text = strip_headers(load_etext(etextno=book_id,
                                          mirror='http://www.mirrorservice.org/sites/ftp.ibiblio.org/pub/docs/books/gutenberg/')).strip()
          text = ' '.join(' '.join(' '.join(text.split('\n')).split('\t')).split('\r'))
          text = ' '.join(text.split())
          text = clean_text(str(text))
      except:
          try:
              page = requests.get(row['Link'])
              soup = BeautifulSoup(page.content, 'html.parser')
              text_link = 'http://www.gutenberg.org' + soup.find_all("a", string="Plain Text UTF-8")[0]['href']
              http_response_object = urlopen(text_link)

              text = strip_headers(str(http_response_object.read()))
              text = ' '.join(' '.join(' '.join(text.split('\n')).split('\t')).split('\r'))
              text = ' '.join(text.split())
              text = clean_text(str(text))
          except:
              print("Couldn't acquire text for " + row['Title'] + ' with ID ' + str(book_id) + '. Link: ' + row['Link'])

      if data['Text'] == None:
          data['Text'] = [' '.join(text.split(' '))]
      else:
          try:
              data['Text'].append(' '.join(text.split(' ')))
          except:
              data['Text'].append(None)
              print("Couldn't save data for " + row['Title'] + ' with ID ' + str(book_id) + '. Link: ' + row['Link'])

  df_data = pd.DataFrame(data, columns = ['Title', 'Author', 'Link', 'ID', 'Bookshelf', 'Text'])
  df_data.to_csv('gutenberg_data/' + output_name + '.csv', index=False)

if __name__ == '__main__': # Executed only on main process.

  print("number of cpu's: " + str(cpu_count()))

  NO_SHARDS = 128

  shard_output_names = ["gutenberg_data_" + str(i) for i in range(NO_SHARDS)]
  shard_ids = [i for i in range(NO_SHARDS)]

  ctx = mp.get_context('spawn')

  proccesses = []

  for i in range(NO_SHARDS):
    proccesses.append(ctx.Process(target=load_gutenburg_shard, args=(shard_output_names[i], shard_ids[i], NO_SHARDS)))

  for p in proccesses:
    p.start()

  for p in proccesses:
    p.join()