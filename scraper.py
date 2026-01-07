import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

BASE_URL = "https://www.hcpcsdata.com"
CODES_URL = f"{BASE_URL}/Codes"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
}
session = requests.Session()
def get_category_name_mapping():
    response = session.get(CODES_URL, headers=HEADERS)
    soup = BeautifulSoup(response.content, 'html.parser')
    code_map = {}

    rows = soup.find_all("tr")
    for row in rows:
        cells = row.find_all("td")
        if len(cells) >= 3:
            code_prefix = cells[0].text.strip().replace(" Codes", "").strip("' ")
            category_name = cells[2].text.strip()
            code_map[code_prefix] = category_name
    return code_map

def get_short_description(detail_url):
    try:
        res = session.get(detail_url, headers=HEADERS)
        soup = BeautifulSoup(res.content, 'html.parser')

        for row in soup.find_all("tr"):
            tds = row.find_all("td")
            if len(tds) == 2:
                label = tds[0].get_text(strip=True).lower()
                if 'short description' in label:
                    return tds[1].get_text(strip=True)
    except Exception as e:
        print(f" Error fetching short description from {detail_url}: {e}")
    return ""


def get_code_page_links():
    response = session.get(CODES_URL, headers=HEADERS)
    soup = BeautifulSoup(response.content, 'html.parser')
    category_links = []

    for a in soup.find_all("a", href=True):
        href = a.get("href").strip()
        text = a.text.strip()
        if href.startswith("/Codes/") and len(href) == 8:
            code_letter = href.split("/")[-1] 
            category_links.append({
                "group": f"HCPCS '{code_letter}' Codes",
                "prefix": code_letter,
                "url": BASE_URL + href
            })

    return category_links

def parse_code_table(category, group, url):
    codes = []
    seen_codes = set()
    page = 1

    while True:
        page_url = f"{url}?page={page}"
        print(f"Scraping {page_url}...")
        res = session.get(page_url, headers=HEADERS)
        soup = BeautifulSoup(res.content, 'html.parser')

        rows = soup.select("table.table tbody tr")
        print(f"Page {page}: Found {len(rows)} rows")

        if not rows:
            break

        new_codes = 0

        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 2:
                continue

            code = cols[0].text.strip()
            if code in seen_codes:
                continue
            seen_codes.add(code)

            desc = cols[1].text.strip()
            detail_link = cols[0].find("a")
            short_desc = ""

            if detail_link and 'href' in detail_link.attrs:
                detail_url = BASE_URL + detail_link['href']
                short_desc = get_short_description(detail_url)

            codes.append({
                "Group": group,
                "Category": category,
                "Code": code,
                "Long Description": desc,
                "Short Description": short_desc if short_desc else desc
            })
            new_codes += 1

        if new_codes == 0:
            print(f" No new codes on page {page}. Ending pagination.")
            break

        page += 1
        time.sleep(0.2)

    print(f" Scraped {len(codes)} codes from {url}")
    return codes


def main():
    all_codes = []

    prefix_to_category = get_category_name_mapping()
    category_links = get_code_page_links()

    for cat in category_links:
        prefix = cat["prefix"]
        category_name = prefix_to_category.get(prefix, "Unknown")
        codes = parse_code_table(category_name, cat["group"], cat["url"])
        print(f"Scraped {len(codes)} codes from {cat['url']}")
        all_codes.extend(codes)

    df = pd.DataFrame(all_codes)
    df.to_csv("hcpcs_codes2.csv", index=False)
    print(f"\nFinished! Total codes scraped: {len(df)}")
    print("Saved to: hcpcs_codes2.csv")

if __name__ == "__main__":
    main()