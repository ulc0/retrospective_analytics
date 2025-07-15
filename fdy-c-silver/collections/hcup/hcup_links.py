

hcup_index="https://hcup-us.ahrq.gov/tools_software.jsp"

# Parse the HTML content
soup = BeautifulSoup(html_content, 'html.parser')

# Find all anchor tags
links = soup.find_all('a')

# Get and print the href attribute (link) for each anchor tag
for link in links:
    href = link.get('href')
    if href:
        print(href)