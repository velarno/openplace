"""
Use fetch_new_dce() to store metadata in database and store the archives in the public directory.
Use fetch_current_annonces() to fetch the list of currently available DCE.
Use fetch_data() to fetch the metadata and the files custituting a DCE.
"""

import re
from collections import Counter
import logging
import duckdb
import json
from typing import Any
import datetime
import traceback
import os

import requests
from bs4 import BeautifulSoup

from config import build_internal_filepath


URL_SEARCH = 'https://www.marches-publics.gouv.fr/?page=Entreprise.EntrepriseAdvancedSearch&AllCons'

PAGE_STATE_REGEX = 'name="PRADO_PAGESTATE" id="PRADO_PAGESTATE" value="([a-zA-Z0-9/+=]+)"'
LINK_REGEX = r'^https://www\.marches-publics\.gouv\.fr/app\.php/entreprise/consultation/([\d]+)\?orgAcronyme=([\da-z]+)$'
REGLEMENT_REGEX = r'^/index.php\?page=Entreprise\.EntrepriseDownloadReglement&id=([a-zA-Z\d=]+)&orgAcronyme=([\da-z]+)$'
BOAMP_REGEX = r'^http://www\.boamp\.fr/(?:index\.php/)?avis/detail/([\d-]+)(?:/[\d]+)?$'

class NoMoreResultsException(Exception):
    pass


def fetch_new_dce() -> None:
    """fetch_new_dce(): Fetch the new DCE and store the metadata in the database.
    """
    pass

def check_content_type(content_type: str, link: str) -> bool:
    # a few DCE have "Content-Type: application/octet-stream" even though they are zip files
    if (content_type not in {'application/zip', 'application/octet-stream'}):
        logging.warning('Unexpected content type {} on {}'.format(content_type, link))
        return False
    return True


def init() -> tuple[list[str], str, str]:
    """init(): Fetch the first page of the row.
    """

    # get page state
    response = requests.get(URL_SEARCH, allow_redirects=False, timeout=600)
    assert response.status_code == 200, response.status_code
    page_state = re.search(PAGE_STATE_REGEX, response.text).groups()[0]
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
    links = extract_links(response, LINK_REGEX)
    page_state = re.search(PAGE_STATE_REGEX, response.text).groups()[0]

    return links, page_state, cookie


def next_page(
    page_state: str, cookie: str, previous_links: list[str]
) -> tuple[list[str], str, str]:
    """
    next_page: Fetch the next page of DCE results.

    Args:
        page_state (str): The current page state token.
        cookie (str): The session cookie.
        previous_links (list[str]): The list of links from the previous page.

    Returns:
        tuple[list[str], str, str]: A tuple containing the list of links, the new page state, and the cookie.

    Raises:
        NoMoreResultsException: If there are no more results to fetch.
    """
    logging.debug(f"Entering next_page with page_state={page_state}, cookie={cookie}, previous_links={previous_links}")

    if not page_state:
        logging.debug("No page_state provided, calling init() to fetch the first page.")
        return init()

    data = {
        'PRADO_PAGESTATE': page_state,
        'PRADO_POSTBACK_TARGET': 'ctl0$CONTENU_PAGE$resultSearch$PagerTop$ctl2',
    }
    logging.debug(f"Posting to {URL_SEARCH} with data={data} and cookie={cookie}")
    
    response = requests.post(
        URL_SEARCH,
        headers={'Cookie': cookie},
        data=data,
        allow_redirects=False,
        timeout=600,
    )

    logging.debug(f"Received response with status_code={response.status_code}")

    if response.status_code == 500:
        logging.debug("Response status code 500, raising NoMoreResultsException.")
        raise NoMoreResultsException()

    assert response.status_code == 200, f"Unexpected status code: {response.status_code}"
    links = extract_links(response, LINK_REGEX)
    logging.debug(f"Extracted {len(links)} links from the response.")

    page_state_new_results = re.search(PAGE_STATE_REGEX, response.text)
    if not page_state_new_results:
        logging.debug("No new page state found in response, raising NoMoreResultsException.")
        raise NoMoreResultsException()
    page_state_new = page_state_new_results.groups()[0]
    logging.debug(f"Extracted new page_state: {page_state_new}")

    if page_state == page_state_new:
        logging.debug("Page state did not change, raising NoMoreResultsException.")
        raise NoMoreResultsException()
    
    if previous_links == links:
        logging.debug("Links did not change from previous page, raising NoMoreResultsException.")
        raise NoMoreResultsException()

    logging.debug(f"Returning {len(links)} links, new page_state, and cookie.")
    return links, page_state_new, cookie


def extract_links(request_result: requests.Response, regex: str) -> list[str]:
    page = request_result.text
    soup = BeautifulSoup(page, 'html.parser')
    links = soup.find_all('a')
    hrefs = [link.attrs['href'] for link in links if 'href' in link.attrs]
    hrefs_clean = [href for href in hrefs if re.match(regex, href)]
    return hrefs_clean


def fetch_current_annonces(nb_pages: int = 0) -> list[str]:
    """fetch_current_annonces(): Fetch the list of currently available DCE.

    nb_pages: number of pages to fetch, 0 to set no limit (for example, you can set to 1 for a development setup)

    Returns a list of URL.
    """
    links_by_page: list[list[str]] = []
    page_state: str | None = None
    cookie: str | None = None
    current_page_links: list[str] | None = None
    try:
        counter: int = 0
        while (nb_pages == 0) or (counter < nb_pages):
            current_page_links, page_state, cookie = next_page(page_state, cookie, current_page_links)
            logging.debug(f'Found {len(current_page_links)} new links')
            links_by_page.append(current_page_links)
            counter += 1

    except NoMoreResultsException:
        pass

    all_links: list[str] = []
    for links in links_by_page:
        all_links += links
    if len(all_links) != len(set(all_links)):
        duplicates: list[str] = [k for k, v in Counter(all_links).items() if v > 1]
        nb_duplicates: int = len(duplicates)
        logging.info(f'{nb_duplicates} DCE found multiple times')
    return all_links



if __name__ == '__main__':
    # Configure logging to output to stdout when running as main
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(levelname)s %(message)s',
        handlers=[logging.StreamHandler()]
    )
    links = fetch_current_annonces(nb_pages=2)
    print(links)


STATE_FETCH_OK = 'ok'

def process_link(link: str) -> int:
    """
    Download data and store it in a DuckDB database.
    Return the number of stored DCE (0 or 1).

    Args:
        link (str): The URL of the DCE to process.

    Returns:
        int: 1 if a new DCE was stored, 0 otherwise.
    """
    # Extract annonce_id from the link using the LINK_REGEX
    match = re.match(LINK_REGEX, link)
    if not match:
        logging.warning(f"Link does not match expected format: {link}")
        return 0
    annonce_id, org_acronym = match.groups()

    # Connect to DuckDB and ensure the table exists
    db_path = "openplace_dce.duckdb"
    con = duckdb.connect(db_path)
    con.execute("""
        CREATE TABLE IF NOT EXISTS dce (
            annonce_id VARCHAR PRIMARY KEY,
            data JSON,
            fetch_datetime TIMESTAMP,
            state VARCHAR DEFAULT 'ok'
        )
    """)

    # Check if the DCE is already processed
    result = con.execute(
        "SELECT COUNT(*) FROM dce WHERE annonce_id = ?",
        [annonce_id]
    ).fetchone()
    if result and result[0] > 0: 
        con.close()
        return 0

    try:
        annonce_data: dict[str, Any] = fetch_data(link)
    except Exception as exception:
        logging.warning("Exception of type {} on {}".format(type(exception).__name__, link))
        logging.debug("Exception details: {}".format(exception))
        logging.debug(traceback.format_exc())
        con.close()
        return 0

    annonce_data['fetch_datetime'] = datetime.datetime.now().isoformat()
    annonce_data['state'] = STATE_FETCH_OK

    # Insert the new DCE into DuckDB
    con.execute(
        "INSERT INTO dce (annonce_id, data, fetch_datetime, state) VALUES (?, ?, ?, ?)",
        [
            annonce_id,
            json.dumps(annonce_data, default=str),
            annonce_data['fetch_datetime'],
            annonce_data['state']
        ]
    )
    con.close()
    return 1


def fetch_data(link_annonce: str) -> dict[str, Any]:
    """fetch_data(): Fetch the metadata and the files of a given DCE.
    """
    # Extract annonce_id and org_acronym from the link using the LINK_REGEX
    match = re.match(LINK_REGEX, link_annonce)
    if not match:
        logging.warning(f"Link does not match expected format: {link_annonce}")
        return {}
    annonce_id, org_acronym = match.groups()

    # Fetch the page
    response = requests.get(link_annonce, allow_redirects=False, timeout=600)
    assert response.status_code == 200

    # Get text data
    links_boamp = extract_links(response, BOAMP_REGEX)
    unique_boamp = list(set(links_boamp))

    # Get soup
    soup = BeautifulSoup(response.text, 'html.parser')

    recap_data = soup.find_all(class_="col-md-10 text-justify")

    assert recap_data[0].find('label').text.strip() == "Référence :"
    reference = recap_data[0].find('div').find('span').text.strip()

    assert recap_data[1].find('label').text.strip() == "Intitulé :"
    intitule = recap_data[1].find('div').find('span').text.strip()

    assert recap_data[2].find('label').text.strip() == "Objet :"
    objet = recap_data[2].find('div').find('span').text.strip()

    assert recap_data[3].find('label').text.strip() == "Organisme :"
    organisme = recap_data[3].find('div').find('span').text.strip()

    # Get links to files
    publicite_tab = soup.find(id='pub')
    assert publicite_tab is not None
    file_links = publicite_tab.find_all('a')

    links_reglements: list[str] = []
    links_dces: list[str] = []
    links_avis: list[str] = []
    links_complements: list[str] = []

    for link in file_links:
        link_href = link.attrs['href']

        if re.match(BOAMP_REGEX, link_href):
            continue
        if not link_href:
            continue

        if 'id' not in link.attrs:
            # "liens directs"
            continue

        link_id = link.attrs['id']

        if link_id == 'linkDownloadReglement':
            links_reglements.append(link_href)
        elif link_id == 'linkDownloadDce':
            links_dces.append(link_href)
        elif link_id == 'linkDownloadAvis':
            links_avis.append(link_href)
        elif link_id == 'linkDownloadComplement':
            links_complements.append(link_href)
        elif link_id == 'linkDownloadDume':
            pass  # "DUME acheteur" does not contain useful information
        else:
            raise Exception(f'Unknown link type {link_id} : {link_href}')

    assert len(links_reglements) <= 1
    link_reglement = links_reglements[0] if links_reglements else None
    assert len(links_dces) <= 1
    link_dce = links_dces[0] if links_dces else None
    # Avis rectificatifs...
    # assert len(links_avis) <= 1
    link_avis = links_avis[0] if links_avis else None
    assert len(links_complements) <= 1
    link_complement = links_complements[0] if links_complements else None


    def write_response_to_file(annonce_id, filename, file_type, response):
        internal_filepath = build_internal_filepath(annonce_id=annonce_id, original_filename=filename, file_type=file_type)
        with open(internal_filepath, 'wb') as file_object:
            for chunk in response.iter_content(8192):
                file_object.write(chunk)
        return os.path.getsize(internal_filepath)

    # Get avis

    filename_avis = None
    file_size_avis = None
    if link_avis:
        response_avis = requests.get(f'https://www.marches-publics.gouv.fr{link_avis}', stream=True, timeout=600)
        assert response_avis.status_code == 200
        regex_attachment = r'^attachment; filename=([^;]+);'
        filename_avis = re.match(regex_attachment, response_avis.headers['Content-Disposition']).groups()[0]

        file_size_avis = write_response_to_file(annonce_id=annonce_id, filename=filename_avis, file_type='avis', response=response_avis)

    # Fetch reglement

    filename_reglement = None
    reglement_ref = None
    file_size_reglement = None
    if link_reglement:
        reglement_ref = re.match(REGLEMENT_REGEX, link_reglement).groups()[0]
        response_reglement = requests.get(f'https://www.marches-publics.gouv.fr{link_reglement}', stream=True, timeout=600)
        assert response_reglement.status_code == 200
        check_content_type(response_reglement.headers['Content-Type'], link_reglement)
        regex_attachment = r'^attachment; filename="([^"]+)";$'
        filename_reglement = re.match(regex_attachment, response_reglement.headers['Content-Disposition']).groups()[0]

        file_size_reglement = write_response_to_file(annonce_id=annonce_id, filename=filename_reglement, file_type='reglement', response=response_reglement)

    # Fetch complement

    filename_complement = None
    file_size_complement = None
    if link_complement:
        response_complement = requests.get(f'https://www.marches-publics.gouv.fr{link_complement}', stream=True, timeout=600)
        assert response_complement.status_code == 200
        regex_attachment = r'^attachment; filename="([^"]+)"'
        filename_complement = re.match(regex_attachment, response_complement.headers['Content-Disposition']).groups()[0]

        file_size_complement = write_response_to_file(annonce_id=annonce_id, filename=filename_complement, file_type='complement', response=response_complement)


    # Get Dossier de Consultation aux Entreprises

    filename_dce = None
    file_size_dce = None
    if link_dce:
        url_dce = f'https://www.marches-publics.gouv.fr/index.php?page=Entreprise.EntrepriseDemandeTelechargementDce&id={annonce_id}&orgAcronyme={org_acronym}'
        response_dce = requests.get(url_dce, allow_redirects=False, timeout=600)
        assert response_dce.status_code == 200
        page_state = re.search(PAGE_STATE_REGEX, response_dce.text).groups()[0]
        cookie = response_dce.headers['Set-Cookie']

        data = {
            'PRADO_PAGESTATE': page_state,
            'PRADO_POSTBACK_TARGET': 'ctl0$CONTENU_PAGE$validateButton',
            'ctl0$CONTENU_PAGE$EntrepriseFormulaireDemande$RadioGroup': 'ctl0$CONTENU_PAGE$EntrepriseFormulaireDemande$choixAnonyme',
        }
        response_dce2 = requests.post(url_dce, headers={'Cookie': cookie}, data=data, allow_redirects=False, timeout=600)
        assert response_dce2.status_code == 200
        page_state = re.search(PAGE_STATE_REGEX, response_dce2.text).groups()[0]

        data = {
            'PRADO_PAGESTATE': page_state,
            'PRADO_POSTBACK_TARGET': 'ctl0$CONTENU_PAGE$EntrepriseDownloadDce$completeDownload',
        }
        response_dce3 = requests.post(url_dce, headers={'Cookie': cookie}, data=data, stream=True, timeout=600)
        assert response_dce3.status_code == 200

        check_content_type(response_dce3.headers['Content-Type'], link_dce)
        regex_attachment = r'^attachment; filename="([^"]+)";$'
        filename_dce = re.match(regex_attachment, response_dce3.headers['Content-Disposition']).groups()[0]

        file_size_dce = write_response_to_file(annonce_id=annonce_id, filename=filename_dce, file_type='dce', response=response_dce3)


    return {
        'annonce_id': annonce_id,
        'org_acronym': org_acronym,
        'links_boamp': links_boamp,
        'reference': reference,
        'intitule': intitule,
        'objet': objet,
        'organisme': organisme,
        'reglement_ref': reglement_ref,
        'filename_reglement': filename_reglement,
        'filename_complement': filename_complement,
        'filename_avis': filename_avis,
        'filename_dce': filename_dce,
        'file_size_reglement': file_size_reglement,
        'file_size_complement': file_size_complement,
        'file_size_avis': file_size_avis,
        'file_size_dce': file_size_dce,
    }