import requests


def main():
    issues_url = "https://postgresweekly.com/issues"
    r = requests.get(issues_url, stream=True)
    filename = "issue_list.html"
    with open(filename, "wb") as f:
        for chunk in r.iter_content(chunk_size=4096):
            f.write(chunk)


if __name__ == "__main__":
    main()
