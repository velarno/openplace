import logging
from typing import Any, Dict
from bs4 import BeautifulSoup
from bs4.element import Tag
import re
import requests

from openplace.tasks.scrape.patterns import BOAMP_REGEX

logger = logging.getLogger(__name__)

def extract_field(info_sections: list[Tag], idx: int, label: str) -> str:
    """
    Extract a field from the recap_data list.

    Args:
        info_sections (list[Tag]): The list of BeautifulSoup elements containing the recap data.
        idx (int): The index of the field to extract.
        label (str): The label of the field to extract.

    Returns:
        str: The extracted field value.

    Raises:
        ValueError: If the expected label is not found or the value is not found.
    """
    section = info_sections[idx]
    found_label = section.find('label')
    if not found_label or found_label.text.strip() != label:
        logger.error(f"Expected label '{label}' at index {idx}, found '{found_label.text.strip() if found_label else None}'")
        raise ValueError(f"Expected label '{label}' at index {idx}, found '{found_label.text.strip() if found_label else None}'")
    value = section.find('div').find('span')
    if not value:
        logger.error(f"Value for label '{label}' not found.")
        raise ValueError(f"Value for label '{label}' not found.")
    return value.text.strip()

def parse_posting_info(content: BeautifulSoup | requests.Response) -> Dict[str, Any]:
    """
    Parse the posting information from the given BeautifulSoup object or requests.Response.

    Args:
        content (BeautifulSoup | requests.Response): The BeautifulSoup object or requests.Response containing the HTML page.

    Returns:
        Dict[str, Any]: A dictionary with keys 'reference', 'title', 'description', and 'organization'.

    Raises:
        AssertionError: If the expected labels are not found in the HTML.
    """
    if isinstance(content, requests.Response):
        content = BeautifulSoup(content.text, 'html.parser')

    info_sections = content.find_all(class_="col-md-10 text-justify")

    logger.debug("Found %d info_sections elements.", len(info_sections))

    reference = extract_field(info_sections, 0, "Référence :")
    logger.debug("Extracted reference: %s", reference)

    title = extract_field(info_sections, 1, "Intitulé :")
    logger.debug("Extracted title: %s", title)

    description = extract_field(info_sections, 2, "Objet :")
    logger.debug("Extracted description: %s", description)

    organization = extract_field(info_sections, 3, "Organisme :")
    logger.debug("Extracted organization: %s", organization)

    posting_info: Dict[str, Any] = {
        "reference": reference,
        "title": title,
        "description": description,
        "organization": organization,
    }
    logger.debug("Parsed posting info: %s", posting_info)
    return posting_info

def infer_link_type(link_id: str) -> str | None:
    """
    Determine the type of link based on the link_id.

    Args:
        link_id (str): The ID of the link.

    Returns:
        str | None: The type of link, or None if the link_id is not recognized.
    """
    if link_id is None:
        return None

    match link_id:
        case 'linkDownloadReglement':
            return 'reglement'
        case 'linkDownloadDce':
            return 'dce'
        case 'linkDownloadAvis':
            return 'avis'
        case 'linkDownloadComplement':
            return 'complement'
        case 'linkDownloadDume':
            return 'dume'
        case _:
            if not is_boamp_link(link_id):
                logger.warning(f"Unknown link type: {link_id}")
            return None

def is_boamp_link(link_href: str) -> bool:
    """
    Check if the link is a BOAMP link.
    """
    return re.match(BOAMP_REGEX, link_href) is not None

def parse_posting_links(content: BeautifulSoup | requests.Response) -> dict[str, list[str]]:
    """
    Parse the posting links from the given BeautifulSoup object or requests.Response.

    Args:
        content (BeautifulSoup | requests.Response): The BeautifulSoup object or requests.Response containing the HTML page.

    Returns:
        tuple[list[str], list[str], list[str], list[str]]: A tuple containing the links to the reglement, dce, avis, and complement files.
    """
    if isinstance(content, requests.Response):
        content = BeautifulSoup(content.text, 'html.parser')

    publicite_tab = content.find(id='pub')
    if publicite_tab is None:
        logger.error("No publicite_tab found.")
        raise ValueError("No publicite_tab found.")

    file_links = publicite_tab.find_all('a')

    links: dict[str, list[str]] = {
        'reglement': [],
        'dce': [],
        'avis': [],
        'complement': [],
        'dume': [],
    }

    for link in file_links:
        link_href = link.attrs['href']

        if not link_href:
            continue

        link_id = link.attrs['id'] if 'id' in link.attrs else None

        inferred_link_type = infer_link_type(link_id)

        if inferred_link_type is None:
            if not is_boamp_link(link_href):
                logger.warning(f"No link type found for link: {link_id} : {link_href}")
            continue
        elif inferred_link_type == 'dume':
            continue
        else:
            links[inferred_link_type].append(link_href)

    return links


def extract_links_from_anchor_tags(request_result: requests.Response, regex: str) -> list[str]:
    """
    Extract all anchor tag hrefs from the response text that match the given regex.

    Args:
        request_result (requests.Response): The HTTP response object containing the HTML page.
        regex (str): The regular expression pattern to match hrefs.

    Returns:
        list[str]: A list of href strings that match the regex.
    """
    logger.debug("Extracting links from response with regex: %s", regex)
    page: str = request_result.text
    soup: BeautifulSoup = BeautifulSoup(page, 'html.parser')
    links = soup.find_all('a')
    hrefs: list[str] = []
    for link in links:
        href = link.attrs.get('href')
        if href is not None:
            hrefs.append(href)
    hrefs_clean: list[str] = [href for href in hrefs if re.match(regex, href)]
    logger.info("Extracted %d links matching regex.", len(hrefs_clean))
    return hrefs_clean