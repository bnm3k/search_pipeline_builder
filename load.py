import argparse
import os
import sys
import re
import datetime
import contextlib
from collections import namedtuple
from urllib.parse import urlparse, urljoin

import requests
from tqdm import tqdm
from bs4 import BeautifulSoup


@contextlib.contextmanager
def nostdout():
    class DummyFile(object):
        file = None

        def __init__(self, file):
            self.file = file

        def write(self, x):
            if len(x.rstrip()) > 0:
                tqdm.write(x, file=self.file)

    save_stdout = sys.stdout
    sys.stdout = DummyFile(sys.stdout)
    yield
    sys.stdout = save_stdout


Entry = namedtuple(
    "Entry", ["title", "author", "content", "main_link", "other_links", "tag"]
)


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
        relateive_url = urljoin(base_url, div.a.get("href"))
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
            issue_url = urljoin(base_url, relative_issue_url)
            res = requests.get(issue_url)
            with open(issue_file_path, "wb") as f:
                f.write(res.content)
            downloaded.append(issue_id)

    print(f"Download: {len(downloaded)}/{len(catalog)} issues")
    print(f"Already present: {len(already_present)}/{len(catalog)} issues")

    return catalog


def remove_query_params(url):
    return urlparse(url)._replace(query=None).geturl()


# newest
def strategy_1(soup):
    content_elem = soup.find("div", id="content")
    if content_elem is None:
        raise Exception(f"div content not present")

    def is_entry(elem):
        # tag must be table
        if elem.name != "table":
            return False
        classes = elem.get_attribute_list("class")
        if "el-item" not in classes and "item" not in classes:
            return False
        return True

    entries = []
    entries_elems = content_elem.find_all(is_entry)

    for entry_elem in entries_elems:
        # get content
        content_elem = entry_elem.find("p", class_="desc")
        content = content_elem.text.strip()

        # get all links
        all_links = []
        for a_elem in entry_elem.find_all("a"):
            all_links.append(a_elem.get("href"))

        # get main link & title
        main_link = None
        title = None
        main_link_elem = content_elem.find("span", class_="mainlink")
        if main_link_elem is not None:
            main_link = main_link_elem.find("a").get("href")
            title = main_link_elem.text
            content = content.removeprefix(title).removeprefix(" — ")
            all_links.remove(main_link)
        elif len(all_links) == 1:
            main_link = all_links[0]
            all_links = []

        # get author name
        author = None
        author_elem = entry_elem.find("p", class_="name") or content_elem.find(
            "span", class_="name"
        )
        if author_elem is not None:
            author = author_elem.text

        if title is None:
            title = content.removesuffix(author).strip()
            content = None
        entry = Entry(
            title=title,
            author=author,
            content=content,
            main_link=main_link,
            other_links=all_links,
            tag=None,
        )
        entries.append(entry)
    return entries


def strategy_2(soup):
    entries_elems = soup.find_all("table", class_="item")
    if len(entries_elems) < 1:
        raise Exception("no entry elems")

    metadata_pattern = re.compile(r"^.*#(\w+).*$")
    entries = []
    for entry_elem in entries_elems:
        # get main link
        main_a_elem = entry_elem.find("a", class_="primary")
        main_link = remove_query_params(main_a_elem.get("href"))
        title = main_a_elem.get_text()

        content_elem = main_a_elem.parent.find_next_sibling("div")
        content = content_elem.text

        other_links = [
            remove_query_params(e.get("href"))
            for e in content_elem.find_all("a")
        ]

        # get author and tag
        metadata_elem = entry_elem.find("td", class_="metadata")
        metadata_text = metadata_elem.get_text().strip()
        match = metadata_pattern.search(metadata_text)
        if match:
            tag = match.group(1)
            author = metadata_text.removesuffix(tag).removesuffix("#").strip()
        else:
            tag = None
            author = metadata_text
        entry = Entry(
            title=title,
            author=author,
            content=content,
            main_link=main_link,
            other_links=other_links,
            tag=tag,
        )
        entries.append(entry)

    return entries


def strategy_3(soup):
    entries = []

    def is_main_link(elem):
        if elem.name != "a":
            return False
        if elem.get("title") is None:
            return False
        return True

    entries_elems = soup.find_all(is_main_link)
    for a_elem in entries_elems:
        main_link = remove_query_params(a_elem.get("href"))
        title = a_elem.get_text()
        tr_content_elem = a_elem.parent.parent.find_next_sibling("tr")
        content = None
        other_links = []
        author = None
        tag = None
        if tr_content_elem is not None:
            content = tr_content_elem.get_text()
            other_links = [
                remove_query_params(e.get("href"))
                for e in tr_content_elem.find_all("a")
            ]
            author_elem = tr_content_elem.find_next_sibling("tr")
            if author_elem is not None:
                author = author_elem.get_text()
        entry = Entry(
            title=title,
            author=author,
            content=content,
            main_link=main_link,
            other_links=other_links,
            tag=tag,
        )
        entries.append(entry)
    return entries


def parse_issue(html_doc):
    soup = BeautifulSoup(html_doc, "html.parser")
    last_exception = None
    for i, strategy in enumerate([strategy_1, strategy_2, strategy_3]):
        try:
            entries = strategy(soup)
            return entries
        except Exception as e:
            last_exception = e
    assert last_exception is not None
    raise last_exception


def assert_schema_entry(e):
    assert e.title is not None and isinstance(e.title, str)
    if e.author is not None:
        assert isinstance(e.author, str)
    if e.content is not None:
        assert isinstance(e.content, str)
    if e.main_link is not None:
        assert isinstance(e.main_link, str)
    if e.tag is not None:
        assert isinstance(e.tag, str)


def main():
    project_root = os.path.dirname(__file__)
    data_dir_default = os.path.join(project_root, "raw_data")
    db_path_default = os.path.join("pg_weekly.db")

    parser = argparse.ArgumentParser(
        prog="load_entries",
        description="retrieves weekly issues from PG Weekly, parses them, loads into a duckdb database then sets up full text search",
    )
    parser.add_argument(
        "--db", help="path to the db file", default=db_path_default
    )
    parser.add_argument(
        "--data_dir",
        help="path to dir where raw html from pg weekly is stored",
        default=data_dir_default,
    )
    args = parser.parse_args()

    base_url = "https://postgresweekly.com"
    # check db path
    db_path = os.path.abspath(args.db)
    if not os.path.isfile(db_path):
        raise Exception(f"Invalid db path: '{db_path}'")
    print(f"DB path: '{db_path}'")

    # check data dir path
    data_dir_path = os.path.abspath(args.data_dir)
    if not os.path.isdir(data_dir_path):
        raise Exception(f"Invalid data dir path: '{data_dir_path}'")
    print(f"Data dir: '{data_dir_path}'")

    catalog = load_catalog(data_dir_path, use_cached=False)

    for issue_id, publish_date, relative_issue_url in tqdm(
        catalog, file=sys.stdout
    ):
        issue_file_path = os.path.join(
            data_dir_path, "issues", f"issue_{issue_id}.html"
        )
        entries = None
        with open(issue_file_path, "rb") as f, nostdout():
            html_doc = f.read()
            try:
                entries = parse_issue(html_doc)
            except Exception as e:
                tqdm.write(
                    f"unable to parse issue: {issue_id}",
                )
        assert entries is not None
        for entry in entries:
            assert_schema_entry(entry)


if __name__ == "__main__":
    main()
