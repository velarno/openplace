import logging
from typing import Iterator
import requests
import re
from openplace.tasks.scrape.parse import extract_links_from_anchor_tags
from openplace.tasks.scrape.patterns import URL_SEARCH, PAGE_STATE_REGEX, LINK_REGEX

logger = logging.getLogger(__name__)

class PlacePostingIterator:
    """
    Generator object that lazily fetches batches of deduplicated links from paginated search results.
    Maintains and updates its state (page_state, cookie, previous_links) as attributes.
    Yields the next batch of deduplicated links on each iteration.

    Provides a method to iterate a fixed number of times.
    """

    def __init__(self) -> None:
        """
        Initialize the generator by fetching the first page and setting initial state.
        """
        # get page state
        response = requests.get(URL_SEARCH, allow_redirects=False, timeout=600)
        assert response.status_code == 200, response.status_code
        match_page_state = re.search(PAGE_STATE_REGEX, response.text)
        if not match_page_state:
            logger.error("Could not extract PRADO_PAGESTATE from initial page.", response.text)
            raise RuntimeError("Could not extract PRADO_PAGESTATE from initial page.")
        page_state = match_page_state.groups()[0]
        cookie = response.headers['Set-Cookie']

        # use page with 20 results
        data = {
            'PRADO_PAGESTATE': page_state,
            'PRADO_POSTBACK_TARGET': 'ctl0$CONTENU_PAGE$resultSearch$listePageSizeTop',
            'ctl0$CONTENU_PAGE$resultSearch$listePageSizeTop': 20,
        }
        response = requests.post(
            URL_SEARCH,
            headers={'Cookie': cookie},
            data=data,
            allow_redirects=False,
            timeout=600,
        )
        assert response.status_code == 200, response.status_code
        links = extract_links_from_anchor_tags(response, LINK_REGEX)
        match_page_state = re.search(PAGE_STATE_REGEX, response.text)
        if not match_page_state:
            logger.error("Could not extract PRADO_PAGESTATE from page after setting page size.", response.text)
            raise RuntimeError("Could not extract PRADO_PAGESTATE from page after setting page size.")
        page_state = match_page_state.groups()[0]

        self.links = self._deduplicate_links(links)
        self.page_state = page_state
        self.cookie = cookie
        self.previous_links: list[str] | None = None
        self._exhausted = False
        self._seen_links: set[str] = set(self.links)

    def __iter__(self) -> Iterator[list[str]]:
        """
        Return the iterator object itself.

        Returns:
            Iterator[list[str]]: The iterator object.
        """
        return self

    def __next__(self) -> list[str]:
        """
        Fetch and yield the next batch of deduplicated links, updating internal state.
        Raises StopIteration when no more results are available.

        Returns:
            list[str]: The next batch of deduplicated links.

        Raises:
            StopIteration: If there are no more results.
        """
        if self._exhausted:
            raise StopIteration
        # On first call, yield the initial links
        if self.previous_links is None:
            self.previous_links = self.links
            return self.links
        # Inline the old next_page logic
        data = {
            'PRADO_PAGESTATE': self.page_state,
            'PRADO_POSTBACK_TARGET': 'ctl0$CONTENU_PAGE$resultSearch$PagerTop$ctl2',
        }
        response = requests.post(
            URL_SEARCH,
            headers={'Cookie': self.cookie},
            data=data,
            allow_redirects=False,
            timeout=600,
        )
        if response.status_code == 500:
            self._exhausted = True
            raise StopIteration
        assert response.status_code == 200, response.status_code
        links = extract_links_from_anchor_tags(response, LINK_REGEX)
        match_page_state = re.search(PAGE_STATE_REGEX, response.text)
        if not match_page_state:
            self._exhausted = True
            raise StopIteration
        page_state = match_page_state.groups()[0]
        if self.page_state == page_state:
            self._exhausted = True
            raise StopIteration
        if self.previous_links == links:
            self._exhausted = True
            raise StopIteration

        deduped_links = self._deduplicate_links(links)
        if not deduped_links:
            self._exhausted = True
            raise StopIteration

        self.links = deduped_links
        self.page_state = page_state
        self.cookie = self.cookie  # cookie may not change
        self.previous_links = links
        self._seen_links.update(deduped_links)
        return deduped_links

    def _deduplicate_links(self, links: list[str]) -> list[str]:
        """
        Deduplicate links, only returning those not seen before.

        Args:
            links (list[str]): The list of links to deduplicate.

        Returns:
            list[str]: Deduplicated list of links.
        """
        deduped = [link for link in links if link not in getattr(self, '_seen_links', set())]
        return deduped

    def iter_n_batches(self, n: int) -> Iterator[list[str]]:
        """
        Iterate over at most n batches of deduplicated links.

        Args:
            n (int): The maximum number of batches to yield.

        Yields:
            Iterator[list[str]]: Up to n batches of deduplicated links.
        """
        count = 0
        for batch in self:
            if count >= n:
                break
            yield batch
            count += 1