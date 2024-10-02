import abc
from typing import Iterable, List, Any, Iterator, Mapping
try:
    from typing import Protocol
except ImportError:
    Protocol = object
from bs4 import BeautifulSoup, Tag as bs4_Tag # type: ignore
from requests.structures import CaseInsensitiveDict

def _raise_not_dict(obj:Any, name:str) -> None:
    try:
        list(obj.keys())
    except AttributeError:
        raise ValueError(f"{name} must be a dictionary-like object")

class ITag(Protocol):
    """
    An HTML tag, decoupled from any particular HTTP library's API.
    """
    name: str
    attributes: Mapping[str, str]
    inner_html: str

class BaseTag(ITag, abc.ABC):
    """
    Subclasses must implement inner_html().
    """
    def __init__(self, name:str, attributes:Mapping[str, str]) -> None:
        _raise_not_dict(attributes, "attributes")
        self.name = name
        self.attributes = attributes
    @property
    def inner_html(self) -> str: # type: ignore
        """Returns the inner HTML of an element as a UTF-8 encoded bytestring"""
        raise NotImplementedError()

class IWebPage(Protocol):
    """
    Interface declaring the required methods/attributes of a WebPage object.
    Simple representation of a web page, decoupled from any particular HTTP library's API.
    """
    url: str
    html: str
    headers: Mapping[str, str]
    scripts: List[str]
    meta: Mapping[str, str]
    def select(self, selector:str) -> Iterable[ITag]:
        raise NotImplementedError()

class BaseWebPage(IWebPage):
    """
    Implements factory methods for a WebPage.

    Subclasses must implement _parse_html() and select(string).
    """

    def __init__(self, url: str, html: str, headers: Mapping[str, str]):
        """
        Initialize a new WebPage object manually.

        :param url: The web page URL.
        :param html: The web page content (HTML)
        :param headers: The HTTP response headers
        """

        _raise_not_dict(headers, "headers")
        self.url = url
        self.html = html
        self.headers = CaseInsensitiveDict(headers)
        self.scripts: List[str] = []
        self.meta: Mapping[str, str] = {}
        self._parse_html()

    def _parse_html(self):
        raise NotImplementedError()


class Tag(BaseTag):

    def __init__(self, name: str, attributes: Mapping[str, str], soup: bs4_Tag) -> None:
        super().__init__(name, attributes)
        self._soup = soup

    def inner_html(self) -> str:
        return self._soup.decode_contents()

class WebPage(BaseWebPage):
    """
    Simple representation of a web page, decoupled from any particular HTTP library's API.

    It will parse the HTML from CommonCrawl archive with BeautifulSoup to find <script> and <meta> tags.
    """

    def _parse_html(self):
        """
        Parse the HTML with BeautifulSoup to find <script> and <meta> tags.
        """
        self._parsed_html = soup = BeautifulSoup(self.html, 'lxml')
        self.scripts.extend(script['src'] for script in
                            soup.findAll('script', src=True))
        self.meta = {
            meta['name'].lower():
                meta['content'] for meta in soup.findAll(
                'meta', attrs=dict(name=True, content=True))
        }

    def select(self, selector: str) -> Iterator[Tag]:
        """Execute a CSS select and returns results as Tag objects."""
        for item in self._parsed_html.select(selector):
            yield Tag(item.name, item.attrs, item)
