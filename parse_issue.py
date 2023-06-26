import os
from collections import namedtuple

from bs4 import BeautifulSoup

from get_issues import get_issues_list, parse_issues_list

Entry = namedtuple(
    "Entry", ["title", "author", "content", "main_link", "other_links"]
)


def parse_entries_A(html_doc):
    soup = BeautifulSoup(html_doc, "html.parser")
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
        )
        entries.append(entry)
    return entries


def get_entries():
    issue_file_path = "data/issues/issue_511.html"
    issue_html_doc = None
    with open(issue_file_path, "r") as f:
        issue_html_doc = f.read()
    entries = parse_entries(issue_html_doc)
    for e in entries:
        print(e, end="\n\n")


def main():
    # config
    data_dir_path = "data/"
    base_url = "https://postgresweekly.com/issues"

    # get list of issues
    list_html_doc = get_issues_list(os.path.join(data_dir_path, "list.html"))
    issues = parse_issues_list(list_html_doc, base_url)
    get_issue_file_path = lambda issue_id: os.path.join(
        data_dir_path, "issues", f"issue_{issue_id}.html"
    )

    failures = []
    for issue in issues:
        issue_id = issue[0]
        issue_html_doc = None
        with open(get_issue_file_path(issue_id), "rb") as f:
            issue_html_doc = f.read()

        try:
            entries = parse_entries_A(issue_html_doc)
        except Exception:
            failures.append(issue)

    # print(f"Able to parse {len(successes)}/{len(issues)} successfully")
    failures.sort(key=lambda t: t[1], reverse=True)
    with open("failures.csv", "w") as f:
        for (issue_id, date, _) in failures:
            f.write(f"{issue_id}, {date} \n")


if __name__ == "__main__":
    main()
