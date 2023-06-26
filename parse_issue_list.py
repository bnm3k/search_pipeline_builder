import calendar
import datetime
import urllib.parse
import re
import timeit

import requests
from bs4 import BeautifulSoup


def main():

    issue_list_url = "https://postgresweekly.com/issues"
    res = requests.get(issue_list_url)
    html_doc = res.content

    soup = BeautifulSoup(html_doc, "html.parser")
    issues_elem = soup.find("div", class_="issues")
    issues = issues_elem.find_all("div", class_="issue")

    pattern = re.compile(
        r"^Issue #(?P<issue_id>\d+)\s+—\s+(?P<month>\w+)\s+(?P<day>\d+)\W+(?P<year>\d+)$"
    )
    base_url = "https://postgresweekly.com"
    month_name_to_num = {m: i for i, m in enumerate(calendar.month_name)}
    links = []
    for div in issues:
        link = urllib.parse.urljoin(base_url, div.a.get("href"))

        text = div.text
        match = pattern.search(div.text)
        if match is None:
            raise Exception(
                f"Cannot parse Issue ID with regex: {pattern.pattern} from text: '{text}'"
            )
        issue_id = int(match.group("issue_id"))
        month_name = match.group("month")
        month_num = month_name_to_num[month_name]
        day = int(match.group("day"))
        year = int(match.group("year"))
        date = datetime.date(year, month_num, day)
        links.append((issue_id, link))

    for (issue_id, issue_url) in links:
        start = timeit.timeit()
        res = requests.get(issue_url)
        filename = f"issues/issue_{issue_id}.html"
        with open(filename, "wb") as f:
            f.write(res.content)
        end = timeit.timeit()
        print(f"downloaded issue: {issue_id}, time taken: {end-start}")


if __name__ == "__main__":
    main()
