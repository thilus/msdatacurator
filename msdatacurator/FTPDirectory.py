import re
import json
import requests

class FTPDirectory:
    """Retrieve information about a PRIDE project.

    PRIDE Archive: `<https://www.ebi.ac.uk/pride/archive/>`_

    Parameters
    ----------
    pride_id : str
        The PRIDE identifier.
    fetch : bool, optional
        Should ppx check the remote repository for updated metadata?
    timeout : float, optional
        The maximum amount of time to wait for a server response.

    Attributes
    ----------
    id : str
    local : Path object
    url : str
    title : str
    description : str
    doi : str
    data_processing_protocol : str
    sample_processing_protocol : str
    metadata : dict
    fetch : bool
    timeout : float
    """

    rest = "https://www.ebi.ac.uk/pride/ws/archive/v2/projects/"
    file_rest = "https://www.ebi.ac.uk/pride/ws/archive/v2/files/byProject"

    def __init__(self, pride_id, local=None, fetch=False, timeout=10.0):
        """Instantiate a PrideDataset object"""
        self.id = self._validate_id(pride_id)
        self.fetch = fetch
        self.timeout = timeout
        self._rest_url = self.rest + self.id
        self._metadata = None
        self._url = None

    def _validate_id(self, identifier):
        """Validate a PRIDE identifier.

        Parameters
        ----------
        identifier : str
            The project identifier to validate.

        Returns
        -------
        str
            The validated identifier
        """
        identifier = str(identifier).upper()
        if not re.match("P[RX]D[0-9]{6}", identifier):
            raise ValueError("Malformed PRIDE identifier.")
        return identifier

    @property
    def url(self):
        """The FTP address associated with this project."""
        if self._url is None:
            links = self.metadata.get("_links", {})
            print(links)
            url = self.metadata["_links"]["datasetFtpUrl"]["href"]
            print("url: " + url)
            fixes = [("", ""), ("/data/", "-"), ("pride.", "")]
            for fix in fixes:
                url = url.replace(*fix)
                try:
                    self._url = self.test_url(url)
                except requests.HTTPError as err:
                    last_error = err
                    continue
                return self._url
            raise last_error
        return self._url

    @property
    def metadata(self):
        """The project metadata as a nested dictionary."""
        if self._metadata is None:
            metadata_file = self.local / ".pride-metadata"
            try:
                if metadata_file.exists():
                    assert self.fetch
                self._metadata = self.get(self._rest_url)
                with metadata_file.open("w+") as ref:
                    json.dump(self._metadata, ref)
            except (AssertionError, requests.ConnectionError) as err:
                if not metadata_file.exists():
                    raise err
                with metadata_file.open() as ref:
                    self._metadata = json.load(ref)
        return self._metadata

    @property
    def title(self):
        """The title of this project."""
        return self.metadata["title"]

    @property
    def description(self):
        """A description of this project."""
        return self.metadata["projectDescription"]

    @property
    def sample_processing_protocol(self):
        """The sample processing protocol for this project."""
        return self.metadata["sampleProcessingProtocol"]

    @property
    def data_processing_protocol(self):
        """The data processing protocol for this project."""
        return self.metadata["dataProcessingProtocol"]

    @property
    def doi(self):
        """The DOI for this project."""
        return self.metadata["doi"]

    @staticmethod
    def get(url, **kwargs):
        """Perform a GET command at the specified url."""
        res = requests.get(url, **kwargs)
        if res.status_code != 200:
            raise requests.HTTPError(f"Error {res.status_code}: {res.text}")
        return res.json()

    @staticmethod
    def test_url(url):
        """Test the accessibility of a URL."""
        res = requests.head(url)
        res.raise_for_status()
        return url


def list_projects(timeout=10.0):
    """List all available projects on PRIDE

    PRIDE Archive: `<https://www.ebi.ac.uk/pride/archive/>`_

    Parameters
    ----------
    timeout : float, optional
        The maximum amount of time to wait for a response from the server.

    Returns
    -------
    list of str
        A list of PRIDE identifiers.
    """
    url = "https://www.ebi.ac.uk/pride/ws/archive/v2/misc/sitemap"
    res = requests.get(url, timeout=timeout)
    if res.status_code != 200:
        raise requests.HTTPError(f"Error {res.status_code}: {res.text})")
    res = [p.split("/")[-1] for p in res.text.splitlines()]
    projects = [p for p in res if re.match("P[RX]D[0-9]{6}", p)]
    projects.sort()
    return projects

def main():
    # Get a list of PRIDE project identifiers
    project_identifiers = list_projects()

    # Iterate over each project identifier
    for identifier in project_identifiers:
        try:
            # Instantiate a FTPDirectory object for the current identifier
            project = FTPDirectory(identifier)

            # Print the project identifier and its associated FTP location
            print(f"Project: {identifier}")
            print(f"FTP Location: {project.url}")
            print()

        except Exception as e:
            print(f"Error processing project {identifier}: {e}")

if __name__ == "__main__":
    main()
