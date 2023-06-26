from bs4 import BeautifulSoup
import urllib.parse
import re


def main():

    html_doc = None
    filename = "issue_list.html"
    with open(filename, "rb") as f:
        html_doc = f.read()

    soup = BeautifulSoup(html_doc, "html.parser")
    issues_elem = soup.find("div", class_="issues")
    issues = issues_elem.find_all("div", class_="issue")

    pattern = re.compile(
        r"^Issue #(?P<issue_id>\d+)\s+—\s+(?P<month>\w+)\s+(?P<day>\d+)\W+(?P<year>\d+)$"
    )
    base_url = "https://postgresweekly.com"
    for div in issues:
        link = urllib.parse.urljoin(base_url, div.a.get("href"))

        text = div.text
        match = pattern.search(div.text)
        if match is None:
            raise Exception(
                f"Cannot parse Issue ID with regex: {pattern.pattern} from text: '{text}'"
            )
        issue_id = int(match.group("issue_id"))
        month = match.group("month")
        day = int(match.group("day"))
        year = int(match.group("year"))
        print(issue_id, month, day, year, link)


if __name__ == "__main__":
    main()
