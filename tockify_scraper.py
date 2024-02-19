import json
import cloudscraper
# from bs4 import BeautifulSoup  
import time

class TockifyEventScraper:
	"""
	Scraper for all Tockify based calendars within UCSD
	
	TODO:
	Create base class on which this class with extend.
	This will be a template for all future calendars
	
	"""

	def __init__(self):
		# Scraper object to bypass AJAX shields
		self.scraper = cloudscraper.create_scraper()

		# Potentially these fields could be inherited from base class
		self.raw_text = ""
		self.events_extracted = {}
	
	def get_list_of_events(self, 
						   calendar_name: str = "ucenevents", 
						   calendar_type: str = "monthly", 
						   search_query: str = "",
						   time_ms: int = -1):
		"""
		Function to extract events from specific calendar
		
		Inputs:
		-- calendar_name: Tockify's calendar name. E.g. University Centers is called 'ucenevents'
		-- calendar_type: Can be 'pinboard', 'agenda' or 'monthly'
			Always choose monthly to get full view of events
		-- search_query: Search query if requested, else left as "" will show all events
		-- time_ms: Start time for query in milliseconds.
			If set to (-1) it will take current time and find all events after that
			
		Output:
		-- events: JSON array of all events, along with tags
		(TODO: clean up array into events_extracted that converts info in standardized format)
		"""
		
		if time_ms == -1:
			time_ms = int(round(time.time() * 1000))
		
		url = f"https://tockify.com/{calendar_name}/{calendar_type}/?start_ms={time_ms}&search={search_query}"
		self.raw_text = self.scraper.get(url).text

		# Find query output section in code
		# NOTE: these were found experimentally by examing HTML dump
		# These can change in the future and might need timely readjustment
		
		start_idx = self.raw_text.find("query")+28
		end_idx = self.raw_text.find("metaData")-2

		# Convert segment into json => creates list of events
		# print(self.raw_text[start_idx: end_idx])
		
		events = json.loads(self.raw_text[start_idx: end_idx])
		
		# Generate Tockify event page URL from eid
		"""
		Example URL: https://tockify.com/ucenevents/detail/4627/1708470000000
		breaks down to ../{calendar_name}/detail/{eid.uid}/{eid.tid}
		"""
		for event in events:
			event['event_url'] = r"https://tockify.com/" + str(calendar_name) + \
								"/" + str(event['eid']['uid']) + "/" + str(event['eid']['tid'])

		return events

if __name__ == '__main__':
	scr = TockifyEventScraper()
	events = scr.get_list_of_events(calendar_name="ucenevents", search_query="")
	print(events)