import torch
from os import listdir
import pandas as pd

class PreTokenizedGutenbergDataset(torch.utils.data.Dataset):

  def __init__(self, data_directory, tokenizer):

    shard_names = [name for name in listdir(data_directory) if name.startswith('gutenberg')]

    self.data = []
    for name in shard_names:

        shard_data = pd.read_csv(data_directory + '/' + name)

        shard_data = shard_data.dropna(subset=['Text'])

        for book_text in shard_data['Text']:
            self.data.append(tokenizer.encode(book_text))        

  def __getitem__(self, index):

    assert index < len(self), "index out of bounds"
    return self.data[index]

  def __len__(self):
    return len(self.data)