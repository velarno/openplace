import logging
from typing import Iterator
import requests
import re
from openplace.tasks.scrape.parse import extract_links_from_anchor_tags
from openplace.tasks.scrape.patterns import URL_SEARCH, PAGE_STATE_REGEX, LINK_REGEX
from openplace.tasks.store.types import StorageType
from openplace.storage.local.queries import list_postings

logger = logging.getLogger(__name__)

class PlacePostingIterator:
    """
    Generator object that lazily fetches batches of deduplicated links from paginated search results.
    Maintains and updates its state (page_state, cookie, previous_links) as attributes.
    Yields the next batch of deduplicated links on each iteration.

    Provides a method to iterate a fixed number of times.
    """

    @classmethod
    def from_storage(cls, storage: StorageType) -> 'PlacePostingIterator':
        """
        Initialize the generator from a database file.
        This allows to resume from a previous state, e.g. after a crash, or to only fetch links that are not in the database.

        Args:
            db_path (str): The path to the database file.

        Returns:
            PlacePostingIterator: The initialized generator.
        """
        stored_links = list_postings(storage=storage)
        return cls(links=[link.url for link in stored_links])

    def _initialize_state(self) -> tuple[requests.Response, str, str]:
        """
        Query the state of the search page.

        Returns:
            tuple[requests.Response, str, str]: The response, page state and cookie.
        """
        response = requests.get(URL_SEARCH, allow_redirects=False, timeout=600)
        assert response.status_code == 200, response.status_code
        match_page_state = re.search(PAGE_STATE_REGEX, response.text)
        if not match_page_state:
            logger.error("Could not extract PRADO_PAGESTATE from initial page.", response.text)
            raise RuntimeError("Could not extract PRADO_PAGESTATE from initial page.")
        page_state = match_page_state.groups()[0]
        cookie = response.headers['Set-Cookie']
        return response, page_state, cookie

    def _increment_state(self, page_state: str, cookie: str, num_results: int = 20) -> tuple[requests.Response, str, str]:
        """
        Send a search request to the search page to increment the state.
        Returns the response, new page state and cookie.

        Args:
            page_state (str): The page state to send the request with.
            cookie (str): The cookie to send the request with.
            num_results (int): The number of results to fetch.

        Returns:
            tuple[requests.Response, str, str]: The response, new page state and cookie.
        """
        data = {
            'PRADO_PAGESTATE': page_state,
            'PRADO_POSTBACK_TARGET': 'ctl0$CONTENU_PAGE$resultSearch$PagerTop$ctl2',
            'ctl0$CONTENU_PAGE$resultSearch$listePageSizeTop': num_results,
        }
        response = requests.post(
            URL_SEARCH,
            headers={'Cookie': cookie},
            data=data,
            allow_redirects=False,
            timeout=600,
        )
        assert response.status_code == 200, response.status_code
        match_page_state = re.search(PAGE_STATE_REGEX, response.text)
        if not match_page_state:
            logger.error("Could not extract PRADO_PAGESTATE from page after setting page size.", response.text)
            raise RuntimeError("Could not extract PRADO_PAGESTATE from page after setting page size.")
        new_page_state = match_page_state.groups()[0]
        return response, new_page_state, cookie

    def __init__(self, links: list[str] | None = None) -> None:
        """
        Initialize the generator by fetching the first page and setting initial state.
        """
        if links is not None:
            self.links = links
            self.page_state = None
            self.cookie = None
            self.previous_links = None
            self._exhausted = False
            self._seen_links: set[str] = set(self.links)
        # get page state and attempt to fetch another batch of links

        response, page_state, cookie = self._initialize_state()

        # use page with 20 results
        response, page_state, cookie = self._increment_state(page_state, cookie, num_results=20)
        links = extract_links_from_anchor_tags(response, LINK_REGEX)

        self.links: list[str] = self._deduplicate_links(links)
        self.page_state = page_state
        self.cookie = cookie
        self.previous_links: list[str] | None = None
        self._exhausted = False
        self._seen_links: set[str] = set(self.links)
        self._batch_index = 0
        self._num_new_links = 0
        self._num_iterations = 0

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
        # TODO: refactor this to use the _increment_state method
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
        self._batch_index += 1
        self._num_new_links += len(deduped_links)
        self._num_iterations += 1
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

    def iter_n_batches(self, n: int, ensure_n_new_links: int | None = None) -> Iterator[list[str]]:
        """
        Iterate over at most n batches of deduplicated links.

        Args:
            n (int): The maximum number of batches to yield.
            ensure_n_new_links (int | None): The number of new links to ensure.
            If set, the iterator will continue until the number of new links is reached.
            If not set, the iterator will stop after n batches.

        Yields:
            Iterator[list[str]]: Up to n batches of deduplicated links.
        """
        if ensure_n_new_links is not None:
            if ensure_n_new_links > self._num_new_links:
                logger.warning(f"Ensuring {ensure_n_new_links} new links, but only {self._num_new_links} new links found so far")
            while self._num_new_links < ensure_n_new_links:
                for batch in self:
                    yield batch
        else:
            for batch in self:
                yield batch
                if self._batch_index+1 >= n:
                    break