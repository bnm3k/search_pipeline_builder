import calendar
import datetime
import urllib.parse
import re
from pathlib import Path
import os

import requests
from tqdm import tqdm
from bs4 import BeautifulSoup


def main():
    # config
    data_dir_arg = "data/"
    base_url = "https://postgresweekly.com/issues"

    # check data dir path
    data_dir_path = os.path.abspath(data_dir_arg)
    if not os.path.isdir(data_dir_path):
        raise Exception(f"Invalid data dir path: '{data_dir_path}'")
    print(f"data dir set to: '{data_dir_path}'")

    # get list of issues
    list_html_doc = None
    list_file_name = "list.html"
    list_file_path = os.path.join(data_dir_path, list_file_name)
    try:
        # check if file exists, if so read
        with open(list_file_path, "rb") as f:
            list_html_doc = f.read()
        print(f"read list.html from file: {list_file_path}")
    except FileNotFoundError:
        # if not downloaded, save as file, then read
        list_url = urllib.parse.urljoin(base_url, "/issues")
        res = requests.get(list_url)
        print(f"downloaded list.html from web: {list_url}")
        list_html_doc = res.content
        with open(list_file_path, "wb") as f:
            f.write(res.content)
            print(f"written list.html to: {list_file_path}")

    # parse list of issues
    soup = BeautifulSoup(list_html_doc, "html.parser")
    div_issues = soup.find("div", class_="issues")
    children = div_issues.find_all("div", class_="issue")
    pattern = re.compile(
        r"^Issue #(?P<issue_id>\d+)\s+—\s+(?P<month>\w+)\s+(?P<day>\d+)\W+(?P<year>\d+)$"
    )
    month_name_to_num = {m: i for i, m in enumerate(calendar.month_name)}
    issues = []
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
        issues.append((issue_id, publish_date, relateive_url))

    # make sure issues dir exists
    issues_dir_path = os.path.join(data_dir_path, "issues")
    if not os.path.isdir(issues_dir_path):
        raise Exception(f"Invalid issues dir path: '{data_dir_path}'")

    # make sure each issue is downloaded
    downloaded = []
    already_present = []
    for (issue_id, publish_date, relative_issue_url) in tqdm(issues):
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

    print(f"downloaded issues: {len(downloaded)}/{len(issues)}")
    print(f"already present issues: {len(already_present)}/{len(issues)}")


if __name__ == "__main__":
    main()
