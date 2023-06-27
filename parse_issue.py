import os
import re
from collections import namedtuple
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from get_issues import get_issues_list, parse_issues_list

Entry = namedtuple(
    "Entry", ["title", "author", "content", "main_link", "other_links", "tag"]
)


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
    for strategy in [strategy_1, strategy_2, strategy_3]:
        try:
            entries = strategy(soup)
            return entries
        except Exception as e:
            last_exception = e
    raise last_exception


def main():
    # config
    base_url = "https://postgresweekly.com/issues"
    list_html_doc = get_issues_list(os.path.join("data/", "list.html"))
    issues = parse_issues_list(list_html_doc, base_url)
    for (issue_id, publish_date, relative_issue_url) in issues:
        issue_file_path = os.path.join("data/issues", f"issue_{issue_id}.html")
        with open(issue_file_path, "rb") as f:
            html_doc = f.read()
            entries = parse_issue(html_doc)


if __name__ == "__main__":
    main()
