from bs4 import BeautifulSoup
soup = BeautifulSoup(open("1000131_raw_html.txt"))
soup.get_text().replace("\n",'')
for script in soup(["script", "style"]):
	script.extract()    # rip it out
text = soup.get_text()
# break into lines and remove leading and trailing space on each
lines = (line.strip() for line in text.splitlines())
# break multi-headlines into a line each
chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
text = '\n'.join(chunk for chunk in chunks if chunk)
print(text)
