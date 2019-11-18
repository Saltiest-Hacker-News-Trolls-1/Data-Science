#!/usr/bin/env python

import html
import logging
import re
import requests

API_LOG = logging.getLogger('root')
ENDPOINT = 'https://hacker-news.firebaseio.com/v0/'


def get_item(id: int, required_keys: set = None, comments_only: bool = True) -> dict:
	"""
	Get an item from the HN API

	Args:
		id (int): HN item ID
		required_keys (set, optional): Keys the item must have

	Returns:
		(dict): Item properties
	"""

	url = f'{ENDPOINT}/item/{id}.json'
	response = requests.get(url)
	assert response.status_code == 200, \
		f'Non-200 response from {url}'
	item = response.json()
	if required_keys is not None:
		assert set(item.keys()) > required_keys
	if comments_only and item['type'] != 'comment':
		return (None)
	return (item)


def get_max_item() -> int:
	"""
	Get the maximum item id from the HN API

	Returns:
		(int): Max item ID
	"""

	url = f'{ENDPOINT}/maxitem.json'
	response = requests.get(url)
	assert response.status_code == 200, \
		f'Non-200 response from {url}'
	return (response.json())



def cleaner_func(comment):
	"""
	Remove HTML elements from comment strings

	Returns:
		(str): comment
	"""
	comment = html.unescape(comment) # remove html escapes
	comment = re.sub('<.*?>',' ',comment) # remove HTML tags
	comment = re.sub('http[s]?://\S+', ' ', comment) # remove links
	return comment
