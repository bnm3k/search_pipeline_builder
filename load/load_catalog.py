import calendar
import datetime
import urllib.parse
import re
import os

import requests
from tqdm import tqdm
from bs4 import BeautifulSoup


def get_issues_catalog(catalog_file_path, use_cached):
    catalog_html_doc = None
    if use_cached:
        with open(catalog_file_path, "rb") as f:
            catalog_html_doc = f.read()
            print(
                f"Read catalog.html from file (might be stale): {catalog_file_path}"
            )
    else:
        # download
        catalog_url = "https://postgresweekly.com/issues"
        res = requests.get(catalog_url)
        print(f"Download latest catalog.html from web: {catalog_url}")
        catalog_html_doc = res.content
        with open(catalog_file_path, "wb") as f:
            f.write(res.content)
            print(f"Write catalog.html to: {catalog_file_path}")
    assert catalog_html_doc is not None
    return catalog_html_doc


def parse_issues_catalog(catalog_html_doc):
    base_url = "https://postgresweekly.com"
    soup = BeautifulSoup(catalog_html_doc, "html.parser")
    div_issues = soup.find("div", class_="issues")
    children = div_issues.find_all("div", class_="issue")
    pattern = re.compile(
        r"^Issue #(?P<issue_id>\d+)\s+—\s+(?P<month>\w+)\s+(?P<day>\d+)\W+(?P<year>\d+)$"
    )
    month_name_to_num = {m: i for i, m in enumerate(calendar.month_name)}
    catalog = []
    for div in children:
        relateive_url = urllib.parse.urljoin(base_url, div.a.get("href"))
        match = pattern.search(div.text)
        if match is None:
            raise Exception(
                f"Cannot parse Issue ID with regex: {pattern.pattern} from text: '{div.text}'"
            )
        issue_id = int(match.group("issue_id"))
        month_name = match.group("month")
        month_num = month_name_to_num[month_name]
        day = int(match.group("day"))
        year = int(match.group("year"))
        publish_date = datetime.date(year, month_num, day)
        catalog.append((issue_id, publish_date, relateive_url))

    return catalog


def load_catalog(data_dir, use_cached=False):
    # config
    base_url = "https://postgresweekly.com"

    # get catalog of issues
    catalog_file_name = "catalog.html"
    catalog_file_path = os.path.join(data_dir, catalog_file_name)
    catalog_html_doc = get_issues_catalog(catalog_file_path, use_cached)

    # parse catalog of issues
    catalog = parse_issues_catalog(catalog_html_doc)

    # make sure issues dir exists
    issues_dir_path = os.path.join(data_dir, "issues")
    if not os.path.isdir(issues_dir_path):
        raise Exception(f"Invalid issues dir path: '{issues_dir_path}'")

    # make sure each issue is downloaded
    downloaded = []
    already_present = []
    for issue_id, publish_date, relative_issue_url in tqdm(catalog):
        issue_file_name = f"issue_{issue_id}.html"
        issue_file_path = os.path.join(issues_dir_path, issue_file_name)
        if os.path.isfile(issue_file_path):
            already_present.append(issue_id)
        else:
            issue_url = urllib.parse.urljoin(base_url, relative_issue_url)
            res = requests.get(issue_url)
            with open(issue_file_path, "wb") as f:
                f.write(res.content)
            downloaded.append(issue_id)

    print(f"Download: {len(downloaded)}/{len(catalog)} issues")
    print(f"Already present: {len(already_present)}/{len(catalog)} issues")

    return catalog
