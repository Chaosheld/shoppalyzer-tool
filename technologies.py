import re
import json
from typing import Callable, Dict, Iterable, Any, Mapping, Set, List, Optional
from fingerprint import Fingerprint, Pattern, Technology, Category
from webpage import WebPage
from helpers import load_json_files

with open('src/categories.json', 'r', encoding='utf-8') as fd:
    categories: Dict[str, Any] = json.load(fd)

#categories: Dict[str, Any] = obj['categories']
#main_technologies: Dict[str, Any] = obj['technologies']
main_technologies: Dict[str, Any] = load_json_files('src/technologies')
categories: Mapping[str, Category] = {k:Category(**v) for k,v in categories.items()}
main_technologies: Mapping[str, Fingerprint] = {k:Fingerprint(name=k, **v) for k,v in main_technologies.items()}

detected_technologies: Dict[str, Dict[str, Technology]] = {}
_confidence_regexp = re.compile(r"(.+)\\;confidence:(\d+)")

def _has_technology(tech_fingerprint: Fingerprint, webpage) -> bool:
    """
    Determine whether the web page matches the technology signature.
    """

    has_tech = False
    # Search the easiest things first and save the full-text search of the
    # HTML for last

    # analyze url patterns
    for pattern in tech_fingerprint.url:
        if pattern.regex.search(webpage.url):
            _set_detected_app(webpage.url, tech_fingerprint, 'url', pattern, value=webpage.url)
    # analyze headers patterns
    for name, patterns in list(tech_fingerprint.headers.items()):
        if name in webpage.headers:
            content = webpage.headers[name]
            if isinstance(content, list):
                for element in content:
                    for pattern in patterns:
                        if pattern.regex.search(element):
                            _set_detected_app(webpage.url, tech_fingerprint, 'headers', pattern, value=element,
                                              key=name)
                            has_tech = True
            else:
                for pattern in patterns:
                    if pattern.regex.search(content):
                        _set_detected_app(webpage.url, tech_fingerprint, 'headers', pattern, value=content, key=name)
                        has_tech = True
    # analyze scripts patterns
    for pattern in tech_fingerprint.scripts:
        for script in webpage.scripts:
            if pattern.regex.search(script):
                _set_detected_app(webpage.url, tech_fingerprint, 'scripts', pattern, value=script)
                has_tech = True
    # analyze meta patterns
    for name, patterns in list(tech_fingerprint.meta.items()):
        if name in webpage.meta:
            content = webpage.meta[name]
            for pattern in patterns:
                if pattern.regex.search(content):
                    _set_detected_app(webpage.url, tech_fingerprint, 'meta', pattern, value=content, key=name)
                    has_tech = True
    # analyze html patterns
    for pattern in tech_fingerprint.html:
        if pattern.regex.search(webpage.html):
            _set_detected_app(webpage.url, tech_fingerprint, 'html', pattern, value=webpage.html)
            has_tech = True
    # analyze dom patterns
    # css selector, list of css selectors, or dict from css selector to dict with some of the keys:
    #           - "exists": "": only check if the selector matches somthing, equivalent to the list form.
    #           - "text": "regex": check if the .innerText property of the element that matches the css selector matches the regex (with version extraction).
    #           - "attributes": {dict from attr name to regex}: check if the attribute value of the element that matches the css selector matches the regex (with version extraction).
    for selector in tech_fingerprint.dom:
        for item in webpage.select(selector.selector):
            if selector.exists:
                _set_detected_app(webpage.url, tech_fingerprint, 'dom', Pattern(string=selector.selector),
                                       value='')
                has_tech = True
            if selector.text:
                for pattern in selector.text:
                    if pattern.regex.search(item.inner_html()):
                        _set_detected_app(webpage.url, tech_fingerprint, 'dom', pattern, value=item.inner_html)
                        has_tech = True
            if selector.attributes:
                for attrname, patterns in list(selector.attributes.items()):
                    _content = item.attributes.get(attrname)
                    if _content:
                        if isinstance(_content, list):
                            for _element in _content:
                                for pattern in patterns:
                                    if pattern.regex.search(_element):
                                        _set_detected_app(webpage.url, tech_fingerprint, 'dom', pattern, value=_element)
                                        has_tech = True
                        else:
                            for pattern in patterns:
                                if pattern.regex.search(_content):
                                    _set_detected_app(webpage.url, tech_fingerprint, 'dom', pattern, value=_content)
                                    has_tech = True

    return has_tech

def _set_detected_app(url: str, tech_fingerprint: Fingerprint, app_type: str, pattern: Pattern,
                      value: str, key='') -> None:
    """
    Store detected technology to the detected_technologies dict.
    """
    # Lookup Technology object in the cache
    if url not in detected_technologies:
        detected_technologies[url] = {}
    if tech_fingerprint.name not in detected_technologies[url]:
       detected_technologies[url][tech_fingerprint.name] = Technology(tech_fingerprint.name)
    detected_tech = detected_technologies[url][tech_fingerprint.name]

    # Set confidence level
    if key != '': key += ' '
    match_name = app_type + ' ' + key + pattern.string

    detected_tech.confidence[match_name] = pattern.confidence

    # Detect version number
    if pattern.version:
        allmatches = re.findall(pattern.regex, value)
        for i, matches in enumerate(allmatches):
            version = pattern.version
            # Check for a string to avoid enumerating the string
            if isinstance(matches, str):
                matches = [(matches)]
            for index, match in enumerate(matches):
                # Parse ternary operator
                ternary = re.search(re.compile('\\\\' + str(index + 1) + '\\?([^:]+):(.*)$', re.I), version)
                if ternary and len(ternary.groups()) == 2 and ternary.group(1) is not None and ternary.group(
                        2) is not None:
                    version = version.replace(ternary.group(0), ternary.group(1) if match != ''
                    else ternary.group(2))
                # Replace back references
                version = version.replace('\\' + str(index + 1), match)
            if version != '' and version not in detected_tech.versions:
                detected_tech.versions.append(version)
        _sort_app_version(detected_tech)

def _sort_app_version(detected_tech: Technology) -> None:
    """
    Sort version number (find the longest version number that *is supposed to* contains all shorter detected version numbers).
    """
    if len(detected_tech.versions) >= 1:
        return
    detected_tech.versions = sorted(detected_tech.versions, key=_cmp_to_key(_sort_app_versions))

def _sort_app_versions(version_a: str, version_b: str) -> int:
    return len(version_a) - len(version_b)

def _cmp_to_key(mycmp: Callable[..., Any]):
    """
    Convert a cmp= function into a key= function
    """

    # https://docs.python.org/3/howto/sorting.html
    class CmpToKey:
        def __init__(self, obj, *args):
            self.obj = obj

        def __lt__(self, other):
            return mycmp(self.obj, other.obj) < 0

        def __gt__(self, other):
            return mycmp(self.obj, other.obj) > 0

        def __eq__(self, other):
            return mycmp(self.obj, other.obj) == 0

        def __le__(self, other):
            return mycmp(self.obj, other.obj) <= 0

        def __ge__(self, other):
            return mycmp(self.obj, other.obj) >= 0

        def __ne__(self, other):
            return mycmp(self.obj, other.obj) != 0

    return CmpToKey

def _get_implied_technologies(detected_technologies: Iterable[str]) -> Iterable[str]:
    """
    Get the set of technologies implied by `detected_technologies`.
    """

    def __get_implied_technologies(technologies: Iterable[str]) -> Iterable[str]:
        _implied_technologies = set()
        for tech in technologies:
            try:
                for implie in main_technologies[tech].implies:
                    # If we have no doubts just add technology
                    if 'confidence' not in implie:
                        _implied_technologies.add(implie)

                    # Case when we have "confidence" (some doubts)
                    else:
                        try:
                            # Use more strict regexp (because we have already checked the entry of "confidence")
                            # Also, better way to compile regexp one time, instead of every time
                            app_name, confidence = _confidence_regexp.search(implie).groups()  # type: ignore
                            if int(confidence) >= 50:
                                _implied_technologies.add(app_name)
                        except (ValueError, AttributeError):
                            pass
            except KeyError:
                pass
        return _implied_technologies

    implied_technologies = __get_implied_technologies(detected_technologies)
    all_implied_technologies: Set[str] = set()

    # Descend recursively until we've found all implied technologies
    while not all_implied_technologies.issuperset(implied_technologies):
        all_implied_technologies.update(implied_technologies)
        implied_technologies = __get_implied_technologies(all_implied_technologies)

    return all_implied_technologies

def analyze(webpage) -> Set[str]:
    """
    Return a set of technology that can be detected on the web page.
    :param webpage: The Webpage to analyze
    """
    detected_technologies = set()

    for tech_name, technology in list(main_technologies.items()):
        if _has_technology(technology, webpage):
            detected_technologies.add(tech_name)

    detected_technologies.update(_get_implied_technologies(detected_technologies))

    return detected_technologies

def get_categories(tech_name: str) -> List[str]:
    """
    Returns a list of the categories for a technology name.

    :param tech_name: Tech name
    """
    cat_nums = main_technologies[tech_name].cats if tech_name in main_technologies else []
    cat_names = [categories[str(cat_num)].name
                 for cat_num in cat_nums if str(cat_num) in categories]
    return cat_names

def get_versions(url: str, app_name: str) -> List[str]:
    """
    Retuns a list of the discovered versions for an app name.

    :param url: URL of the webpage
    :param app_name: App name
    """
    try:
        return detected_technologies[url][app_name].versions
    except KeyError:
        return []

def get_confidence(url: str, app_name: str) -> Optional[int]:
    """
    Returns the total confidence for an app name.

    :param url: URL of the webpage
    :param app_name: App name
    """
    try:
        return detected_technologies[url][app_name].confidenceTotal
    except KeyError:
        return None

def analyze_with_versions(webpage) -> Dict[str, Dict[str, Any]]:
    """
    Return a dict of applications and versions that can be detected on the web page.

    :param webpage: The Webpage to analyze
    """
    detected_apps = analyze(webpage)
    versioned_apps = {}

    for app_name in detected_apps:
        versions = get_versions(webpage.url, app_name)
        versioned_apps[app_name] = {"versions": versions}

    return versioned_apps

def analyze_with_versions_and_categories(webpage) -> Dict[str, Dict[str, Any]]:
    """
    Return a dict of applications and versions and categories that can be detected on the web page.

    :param webpage: The Webpage to analyze
    """
    versioned_apps = analyze_with_versions(webpage)
    versioned_and_categorised_apps = versioned_apps

    for app_name in versioned_apps:
        cat_names = get_categories(app_name)
        versioned_and_categorised_apps[app_name]["categories"] = cat_names

    return versioned_and_categorised_apps

def get_technology(url, html, headers) -> Dict[str, Dict[str, Any]]:
    w = WebPage(url,  html=html, headers=headers)

    return analyze_with_versions_and_categories(w)