import re
import json
import os
import pickle
import asyncio
from tqdm.asyncio import tqdm
from typing import Dict, Iterable, Any, Mapping, Set, List
from fingerprint import Fingerprint, Pattern, Technology, Category
from webpage import WebPage
from helpers import load_json_files
from bs4 import BeautifulSoup

with open('src/categories.json', 'r', encoding='utf-8') as fd:
    categories: Dict[str, Any] = json.load(fd)

CACHE_FILE = 'pattern_cache.pkl'

main_technologies: Dict[str, Any] = load_json_files('src/technologies')
categories: Mapping[str, Category] = {k: Category(**v) for k, v in categories.items()}
main_technologies: Mapping[str, Fingerprint] = {k: Fingerprint(name=k, **v) for k, v in main_technologies.items()}

detected_technologies: Dict[str, Dict[str, Technology]] = {}
_confidence_regexp = re.compile(r"(.+)\\;confidence:(\d+)")

# cache for compiled regex patterns
pattern_cache = {}

def load_pattern_cache():
    """Loads pattern cache from file if exists."""
    global pattern_cache
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "rb") as f:
            pattern_cache = pickle.load(f)


def save_pattern_cache():
    """Stores pattern cache to file."""
    with open(CACHE_FILE, "wb") as f:
        pickle.dump(pattern_cache, f)


def precompile_patterns():
    """Compiles all regex patterns if not already in cache."""
    updated = False
    for tech in main_technologies.values():
        for pattern_list in [tech.url, tech.headers.values(), tech.meta.values(), tech.html, tech.scripts, tech.dom]:
            for patterns in pattern_list:
                for pattern in (patterns if isinstance(patterns, list) else [patterns]):
                    if pattern.string not in pattern_cache:
                        get_compiled_pattern(pattern)
                        updated = True
    if updated:
        save_pattern_cache()


def get_compiled_pattern(pattern: Pattern):
    """Returns pre-compiled regex patterns."""
    if pattern.string not in pattern_cache:
        try:
            pattern_cache[pattern.string] = re.compile(pattern.string)
        except re.error as e:
            print(f"⚠️ Invalid RegEx in pattern: {pattern.string} → {e}")
            pattern_cache[pattern.string] = None
    return pattern_cache.get(pattern.string, None)


load_pattern_cache()

async def _get_dom_cache(webpage) -> BeautifulSoup:
    """Parsing the DOM just once and stores it in cache."""
    if not hasattr(webpage, '_dom_cache'):
        webpage._dom_cache = BeautifulSoup(webpage.html, "html.parser")
    return webpage._dom_cache

async def _has_technology_async(tech_fingerprint: Fingerprint, webpage, enable_dom: bool) -> bool:
    """Checks, if a webpage has patterns of certain technologies."""
    for pattern in tech_fingerprint.url:
        if get_compiled_pattern(pattern) and get_compiled_pattern(pattern).search(webpage.url):
            return True

    for name, patterns in tech_fingerprint.headers.items():
        content = webpage.headers.get(name)
        if isinstance(content, list):
            for item in content:
                for pattern in patterns:
                    if get_compiled_pattern(pattern) and get_compiled_pattern(pattern).search(item):
                        return True
        elif isinstance(content, (str, bytes)):
            for pattern in patterns:
                if get_compiled_pattern(pattern) and get_compiled_pattern(pattern).search(content):
                    return True

    for pattern in tech_fingerprint.html:
        if get_compiled_pattern(pattern) and get_compiled_pattern(pattern).search(webpage.html):
            return True

    # optimized DOM check with cache
    dom_tree = await _get_dom_cache(webpage) if enable_dom else None
    if enable_dom and dom_tree:
        dom_selectors = [selector.selector for selector in tech_fingerprint.dom]
        if dom_selectors:
            for selector in tech_fingerprint.dom:
                elements = dom_tree.select(selector.selector)
                if selector.exists and elements:
                    return True
                if selector.text:
                    for pattern in selector.text:
                        if any(get_compiled_pattern(pattern) and get_compiled_pattern(pattern).search(elem.text or '') for
                               elem in elements):
                            return True
                if selector.attributes:
                    for attrname, patterns in selector.attributes.items():
                        for elem in elements:
                            attr_value = elem.get(attrname, '')
                            for pattern in patterns:
                               if get_compiled_pattern(pattern) and isinstance(attr_value, (str, bytes)) and get_compiled_pattern(
                                        pattern).search(attr_value):
                                    return True
    return False

async def analyze_webpage(webpage, enable_dom: bool) -> Set[str]:
    """Checks all entries from technology lookup asynchronously."""
    detected_techs = set()
    results = []

    tasks = [
        asyncio.create_task(_has_technology_async(tech_fingerprint, webpage, enable_dom))
        for tech_name, tech_fingerprint in main_technologies.items()
    ]

    # fancy progress bar to make the huge amount of checks visible
    results += await tqdm.gather(*tasks)
    for tech_name, has_tech in zip(main_technologies.keys(), results):
        if has_tech:
            detected_techs.add(tech_name)

    detected_techs.update(_get_implied_technologies(detected_techs))
    return detected_techs


def _get_implied_technologies(detected_technologies: Iterable[str]) -> Iterable[str]:
    """Returns technologies which are implied by found technologies based on lookup."""
    implied_technologies = set()
    for tech in detected_technologies:
        if tech in main_technologies:
            implied_technologies.update(main_technologies[tech].implies)
    return implied_technologies


async def analyze_with_versions_and_categories(webpage, enable_dom: bool) -> Dict[str, Dict[str, Any]]:
    detected_apps = await analyze_webpage(webpage, enable_dom)
    versioned_apps = {app: {"versions": get_versions(webpage.url, app)} for app in detected_apps}
    for app_name in versioned_apps:
        versioned_apps[app_name]["categories"] = get_categories(app_name)
    return versioned_apps


def get_versions(url: str, app_name: str) -> List[str]:
    return detected_technologies.get(url, {}).get(app_name, Technology(app_name)).versions


def get_categories(tech_name: str) -> List[str]:
    cat_nums = main_technologies[tech_name].cats if tech_name in main_technologies else []
    return [categories[str(cat_num)].name for cat_num in cat_nums if str(cat_num) in categories]


def get_technology(url, html, headers, enable_dom: bool) -> Dict[str, Dict[str, Any]]:
    w = WebPage(url, html=html, headers=headers)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    return loop.run_until_complete(analyze_with_versions_and_categories(w, enable_dom))