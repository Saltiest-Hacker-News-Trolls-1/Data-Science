#!/usr/bin/env python

import asyncio
import html
import logging
import re
import requests

from aiohttp import ClientSession

API_LOG = logging.getLogger('root')
ENDPOINT = 'https://hacker-news.firebaseio.com/v0/'


async def fetch(url, session):
	"""Fetch a url, using specified ClientSession."""
	async with session.get(url) as response:
		response = await response.json()
		return (response)


async def fetch_all(urls):
	"""Launch requests for all web pages."""
	tasks = []
	async with ClientSession() as session:
		for url in urls:
			task = asyncio.ensure_future(fetch(url, session))
			tasks.append(task)  # create list of tasks
		results = await asyncio.gather(*tasks)  # gather task responses
	return (results)


def fetch_batch(urls, required_keys: set = None, comments_only: bool = True):
	results = asyncio.run(fetch_all(urls))
	batch = {}
	for result in results:
		try:
			if result is None:
				API_LOG.warning(f'Null return from API.')
				continue
			if required_keys is not None and not set(result.keys()) >= required_keys:
				if 'id' in result:
					API_LOG.info(f'Warning while getting item {result["id"]}:')
				API_LOG.warning(f'Keys {set(result.keys())} < required keys {required_keys}')
			elif comments_only and result['type'] != 'comment':
				batch[result['id']] = None
			else:
				batch[result['id']] = result
		except Exception as e:
			API_LOG.info(f'Exception processing result: {result}')
			API_LOG.info(f'Current urls: {urls}')
			API_LOG.exception(e)
			raise
	return (batch)


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
	if required_keys is not None and not set(item.keys()) >= required_keys:
		raise KeyError(f'Keys {set(item.keys())} < required keys {required_keys}')
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
	comment = html.unescape(comment)  # remove html escapes
	comment = comment.replace('\x00', '').replace('\0', '')
	comment = re.sub('<.*?>',' ',comment)  # remove HTML tags
	comment = re.sub('http[s]?://\S+', ' ', comment)  # remove links
	return comment
