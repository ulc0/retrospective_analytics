#https://stackabuse.com/parsing-xml-with-beautifulsoup-in-python/

#from bs4 import BeautifulSoup

with open('teachers.xml', 'r') as f:
    file = f.read() 

# 'xml' is the parser used. For html files, which BeautifulSoup is typically used for, it would be 'html.parser'.
#soup = BeautifulSoup(file, 'xml')

#names = soup.find_all('name')
#for name in names:
#    print(name.text)
#https://lxml.de/api.html
from lxml import etree
for element in root.iter():
     print(f"{element.tag} - {element.text}")


#from collections import deque
#queue = deque([root])
#while queue:
#    el = queue.popleft()  # pop next element
#    queue.extend(el)      # append its children
#    print(el.tag)
