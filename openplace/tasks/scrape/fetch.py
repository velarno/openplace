import logging
import re
import requests
from typing import Callable

from openplace.tasks.scrape.patterns import LINK_REGEX, PAGE_STATE_REGEX
logger = logging.getLogger(__name__)

def fetch_posting_page(link_posting: str) -> tuple[str, str, requests.Response]:
    """
    Validate the link_posting using the provided regex, log the process, and fetch the page.

    Args:
        link_posting (str): The URL of the PLACE public market posting.

    Returns:
        Tuple[str, str, requests.Response]: posting_id, org_acronym, and the HTTP response object.

    Raises:
        ValueError: If the link does not match the expected format or the page fetch fails.
    """
    match = re.match(LINK_REGEX, link_posting)
    if not match:
        logger.error(f"Link does not match expected format: {link_posting}")
        raise ValueError(f"Link does not match expected format: {link_posting}")
    posting_id, org_acronym = match.groups()
    logger.debug(f"Extracted posting_id={posting_id}, org_acronym={org_acronym} from link.")

    try:
        response = requests.get(link_posting, allow_redirects=False, timeout=600)
    except Exception as exc:
        logger.error(f"Exception occurred while fetching page: {link_posting} - {exc}")
        raise

    if response.status_code != 200:
        logger.error(f"Failed to fetch page: {link_posting} (status {response.status_code})")
        raise ValueError(f"Failed to fetch page: {link_posting} (status {response.status_code})")

    logger.debug(f"Successfully fetched page: {link_posting} (status {response.status_code})")
    return posting_id, org_acronym, response

def is_zip_file(response: requests.Response) -> bool:
    # a few DCE have "Content-Type: application/octet-stream" even though they are zip files
    return (
        response.headers['Content-Type'] == 'application/octet-stream' or
        response.headers['Content-Type'] == 'application/zip'
    )

def fetch_dce_file(
    posting_id: str,
    org_acronym: str,
    file_writer: Callable[[str, str, str, requests.Response], int],
    page_state_regex: str = PAGE_STATE_REGEX,
) -> tuple[str | None, int | None]:
    """
    Fetch the Dossier de Consultation aux Entreprises (DCE) file by navigating the required pages.

    Args:
        annonce_id (str): The ID of the announcement.
        org_acronym (str): The organization acronym.
        link_dce (str): The DCE link fragment from the posting page.
        write_response_to_file (callable): Function to write the response content to a file.
        PAGE_STATE_REGEX (str): Regex pattern to extract PRADO_PAGESTATE.

    Returns:
        tuple[str | None, int | None]: The filename of the DCE and its file size, or (None, None) if not found.

    Raises:
        AssertionError: If any of the HTTP requests fail.
        ValueError: If required headers or page state cannot be extracted.
    """
    url_dce = (
        f'https://www.marches-publics.gouv.fr/index.php?page=Entreprise.EntrepriseDemandeTelechargementDce'
        f'&id={posting_id}&orgAcronyme={org_acronym}'
    )

    # Step 1: Initial GET request to get page state and cookie
    response_dce = requests.get(url_dce, allow_redirects=False, timeout=600)
    assert response_dce.status_code == 200, f"Initial DCE GET failed: {response_dce.status_code}"
    match_page_state = re.search(page_state_regex, response_dce.text)
    if not match_page_state:
        logger.error("Could not extract PRADO_PAGESTATE from initial DCE page.")
        raise ValueError("Could not extract PRADO_PAGESTATE from initial DCE page.")
    page_state = match_page_state.groups()[0]
    cookie = response_dce.headers.get('Set-Cookie')
    if not cookie:
        logger.error("Set-Cookie header not found in initial DCE response.")
        raise ValueError("Set-Cookie header not found in initial DCE response.")

    # Step 2: POST to validateButton to get new page state
    data_validate = {
        'PRADO_PAGESTATE': page_state,
        'PRADO_POSTBACK_TARGET': 'ctl0$CONTENU_PAGE$validateButton',
        'ctl0$CONTENU_PAGE$EntrepriseFormulaireDemande$RadioGroup': 'ctl0$CONTENU_PAGE$EntrepriseFormulaireDemande$choixAnonyme',
    }
    response_after_validation = requests.post(
        url_dce, headers={'Cookie': cookie}, data=data_validate, allow_redirects=False, timeout=600
    )
    assert response_after_validation.status_code == 200, f"ValidateButton POST failed: {response_after_validation.status_code}"
    match_page_state_after_validation = re.search(page_state_regex, response_after_validation.text)
    if not match_page_state_after_validation:
        logger.error("Could not extract PRADO_PAGESTATE from validateButton POST response.")
        raise ValueError("Could not extract PRADO_PAGESTATE from validateButton POST response.")
    page_state = match_page_state_after_validation.groups()[0]

    # Step 3: POST to completeDownload to get the DCE file
    data_download = {
        'PRADO_PAGESTATE': page_state,
        'PRADO_POSTBACK_TARGET': 'ctl0$CONTENU_PAGE$EntrepriseDownloadDce$completeDownload',
    }
    response_after_download = requests.post(
        url_dce, headers={'Cookie': cookie}, data=data_download, stream=True, timeout=600
    )
    assert response_after_download.status_code == 200, f"CompleteDownload POST failed: {response_after_download.status_code}"

    if not is_zip_file(response_after_download):
        logger.error(f"Content-Type is not application/octet-stream or application/zip: {response_after_download.headers['Content-Type']}")
        raise ValueError(f"Content-Type is not application/octet-stream or application/zip: {response_after_download.headers['Content-Type']}")

    regex_attachment = r'^attachment; filename="([^"]+)";$'
    content_disp = response_after_download.headers.get('Content-Disposition', '')
    match_filename = re.match(regex_attachment, content_disp)
    if not match_filename:
        logger.error("Could not extract filename from Content-Disposition header.")
        raise ValueError("Could not extract filename from Content-Disposition header.")
    filename_dce = match_filename.groups()[0]

    file_size_dce = file_writer(
        posting_id,
        filename_dce,
        'dce',
        response_after_download,
    )

    return filename_dce, file_size_dce

def fetch_reglement_file(
    annonce_id: str,
    link_reglement: str,
    write_response_to_file: Callable[[str, str, str, requests.Response], int],
) -> tuple[str | None, int | None]:
    """
    Fetch the reglement file from the given link, save it, and return its filename and size.

    Args:
        annonce_id (str): The ID of the announcement.
        link_reglement (str): The relative URL to the reglement file.
        write_response_to_file (Callable): Function to write the response content to a file.

    Returns:
        Tuple[str | None, int | None]: The filename and file size of the reglement, or (None, None) if not found.
    """
    if not link_reglement:
        logger.info("No reglement link provided for annonce_id=%s.", annonce_id)
        return None, None

    url = f'https://www.marches-publics.gouv.fr{link_reglement}'
    logger.info("Fetching reglement file for annonce_id=%s from URL: %s", annonce_id, url)
    response_reglement = requests.get(url, stream=True, timeout=600)
    if response_reglement.status_code != 200:
        logger.error("Failed to fetch reglement file for annonce_id=%s, status_code=%d", annonce_id, response_reglement.status_code)
        raise ValueError(f"Failed to fetch reglement file: {response_reglement.status_code}")

    regex_attachment = r'^attachment; filename="([^"]+)";$'
    content_disposition = response_reglement.headers.get('Content-Disposition', '')
    match_filename = re.match(regex_attachment, content_disposition)
    if not match_filename:
        logger.error("Could not extract filename from Content-Disposition header for annonce_id=%s.", annonce_id)
        raise ValueError("Could not extract filename from Content-Disposition header.")

    filename_reglement = match_filename.groups()[0]
    logger.info("Saving reglement file '%s' for annonce_id=%s.", filename_reglement, annonce_id)
    file_size_reglement = write_response_to_file(
        annonce_id,
        filename_reglement,
        'reglement',
        response_reglement
    )
    logger.info("Reglement file saved: %s (%d bytes) for annonce_id=%s.", filename_reglement, file_size_reglement, annonce_id)
    return filename_reglement, file_size_reglement

def fetch_complement_file(
    annonce_id: str,
    link_complement: str,
    write_response_to_file: Callable[[str, str, str, requests.Response], int],
) -> tuple[str | None, int | None]:
    """
    Fetch the complement file from the given link, save it, and return its filename and size.

    Args:
        annonce_id (str): The ID of the announcement.
        link_complement (str): The relative URL to the complement file. 
        write_response_to_file (Callable): Function to write the response content to a file.

    Returns:
        Tuple[str | None, int | None]: The filename and file size of the complement, or (None, None) if not found.
    """
    if not link_complement:
        logger.info("No complement link provided for annonce_id=%s.", annonce_id)
        return None, None

    url = f'https://www.marches-publics.gouv.fr{link_complement}'
    logger.info("Fetching complement file for annonce_id=%s from URL: %s", annonce_id, url)
    response_complement = requests.get(url, stream=True, timeout=600)
    if response_complement.status_code != 200:
        logger.error("Failed to fetch complement file for annonce_id=%s, status_code=%d", annonce_id, response_complement.status_code)
        raise ValueError(f"Failed to fetch complement file: {response_complement.status_code}")

    regex_attachment = r'^attachment; filename="([^"]+)"'
    content_disp = response_complement.headers.get('Content-Disposition', '')
    match_filename = re.match(regex_attachment, content_disp)
    if not match_filename:
        logger.error("Could not extract filename from Content-Disposition header for annonce_id=%s.", annonce_id)
        raise ValueError("Could not extract filename from Content-Disposition header.")

    filename_complement = match_filename.groups()[0]
    logger.info("Saving complement file '%s' for annonce_id=%s.", filename_complement, annonce_id)
    file_size_complement = write_response_to_file(
        annonce_id,
        filename_complement,
        'complement',
        response_complement
    )
    logger.info("Complement file saved: %s (%d bytes) for annonce_id=%s.", filename_complement, file_size_complement, annonce_id)
    return filename_complement, file_size_complement

def fetch_avis_file(
    annonce_id: str,
    link_avis: str,
    write_response_to_file: Callable[[str, str, str, requests.Response], int],
) -> tuple[str | None, int | None]:
    """
    Fetch the avis file from the given link, save it, and return its filename and size.

    Args:
        annonce_id (str): The ID of the announcement.
        link_avis (str): The relative URL to the avis file.
        write_response_to_file (Callable): Function to write the response content to a file.

    Returns:
        Tuple[str | None, int | None]: The filename and file size of the avis, or (None, None) if not found.
    """
    if not link_avis:
        logger.info("No avis link provided for annonce_id=%s.", annonce_id)
        return None, None

    url = f'https://www.marches-publics.gouv.fr{link_avis}'
    logger.info("Fetching avis file for annonce_id=%s from URL: %s", annonce_id, url)
    response_avis = requests.get(url, stream=True, timeout=600)
    if response_avis.status_code != 200:
        logger.error("Failed to fetch avis file for annonce_id=%s, status_code=%d", annonce_id, response_avis.status_code)
        raise ValueError(f"Failed to fetch avis file: {response_avis.status_code}")

    regex_attachment = r'^attachment; filename=([^;]+);'
    content_disposition = response_avis.headers.get('Content-Disposition', '')
    match_filename = re.match(regex_attachment, content_disposition)
    if not match_filename:
        logger.error("Could not extract filename from Content-Disposition header for annonce_id=%s.", annonce_id)
        raise ValueError("Could not extract filename from Content-Disposition header.")

    filename_avis = match_filename.groups()[0]
    logger.info("Saving avis file '%s' for annonce_id=%s.", filename_avis, annonce_id)
    file_size_avis = write_response_to_file(
        annonce_id,
        filename_avis,
        'avis',
        response_avis
    )
    logger.info("Avis file saved: %s (%d bytes) for annonce_id=%s.", filename_avis, file_size_avis, annonce_id)
    return filename_avis, file_size_avis


class PostingFileFetcher:  
    def __init__(self, posting_id: str, org_acronym: str, file_writer: Callable[[str, str, str, requests.Response], int]):
        self.posting_id = posting_id
        self.org_acronym = org_acronym
        self.file_writer = file_writer

    @staticmethod
    def dce(
        posting_id: str,
        org_acronym: str,
        file_writer: Callable[[str, str, str, requests.Response], int],
        page_state_regex: str = PAGE_STATE_REGEX,
    ) -> tuple[str | None, int | None]:
        return fetch_dce_file(posting_id, org_acronym, file_writer, page_state_regex)
    
    @staticmethod
    def reglement(
        posting_id: str,
        link_reglement: str,
        write_response_to_file: Callable[[str, str, str, requests.Response], int],
    ) -> tuple[str | None, int | None]:
        return fetch_reglement_file(posting_id, link_reglement, write_response_to_file)
    
    @staticmethod
    def complement(
        posting_id: str,
        link_complement: str,
        write_response_to_file: Callable[[str, str, str, requests.Response], int],
    ) -> tuple[str | None, int | None]:
        return fetch_complement_file(posting_id, link_complement, write_response_to_file)
    
    @staticmethod
    def avis(
        posting_id: str,
        link_avis: str,
        write_response_to_file: Callable[[str, str, str, requests.Response], int],
    ) -> tuple[str | None, int | None]:
        return fetch_avis_file(posting_id, link_avis, write_response_to_file)

    def __call__(self, kind: str, link: str) -> tuple[str | None, int | None]:
        """
        Fetch a file from the posting. Can fetch DCE, reglement, complement, or avis.

        Args:
            kind (str): The kind of file to fetch. Can be 'dce', 'reglement', 'complement', or 'avis'.
            link (str): The link to the file to fetch.

        Returns:
            tuple[str | None, int | None]: The filename and file size of the file, or (None, None) if not found.
        """
        if kind not in ['dce', 'reglement', 'complement', 'avis']:
            raise ValueError(f"Unknown file kind: {kind}")
        
        if kind == 'dce':
            return self.dce(self.posting_id, self.org_acronym, self.file_writer)
        elif kind == 'reglement':
            return self.reglement(self.posting_id, link, self.file_writer)
        elif kind == 'complement':
            return self.complement(self.posting_id, link, self.file_writer)
        elif kind == 'avis':
            return self.avis(self.posting_id, link, self.file_writer)
        else:
            raise ValueError(f"Unknown file kind: {kind}")

        