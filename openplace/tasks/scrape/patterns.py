"""
Patterns used for scraping the website urls and maintaining the state of the scraping process.
"""

REGLEMENT_REGEX = r'^/index.php\?page=Entreprise\.EntrepriseDownloadReglement&id=([a-zA-Z\d=]+)&orgAcronyme=([\da-z]+)$'
BOAMP_REGEX = r'^https?://www\.boamp\.fr/(?:index\.php/)?avis/detail/([\d-]+)(?:/[\d]+)?$'
URL_SEARCH = 'https://www.marches-publics.gouv.fr/?page=Entreprise.EntrepriseAdvancedSearch&AllCons'
LINK_REGEX = r'^https://www\.marches-publics\.gouv\.fr/app\.php/entreprise/consultation/([\d]+)\?orgAcronyme=([\da-z]+)$'
PAGE_STATE_REGEX = 'name="PRADO_PAGESTATE" id="PRADO_PAGESTATE" value="([a-zA-Z0-9/+=]+)"'