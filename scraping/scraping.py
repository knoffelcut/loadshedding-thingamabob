import bs4

def soup_me(soup: bs4.BeautifulSoup):
    """Converts soup to a beautifulsoup4 object

    Args:
        soup (bs4.BeautifulSoup or markup): input
    """
    if not isinstance(soup, bs4.BeautifulSoup):
        soup = bs4.BeautifulSoup(soup, 'html.parser')

    return soup

def extract_coct_loadshedding_text(soup: bs4.BeautifulSoup):
    """Extracts the text relevant to the current loadshedding schedule from the CoCT website

    Args:
        soup (bs4.BeautifulSoup or markup): CoCT HTML
    """
    soup = soup_me(soup)

    soup = soup.find_all("p", class_="lrg")[0]
    lines = [line.strip() for line in soup.get_text().splitlines()]
    lines = [line for line in lines if line != '']

    return lines

def extract_eskom_loadshedding_stage(response_text: str):
    """Extracts the current Eskom loadshedding stage

    Args:
        response_text (str): Eskom API response

    Returns:
        int: Current national loadshedding stage
    """
    stage = int(response_text)
    stage = stage - 1
    return stage


if __name__ == '__main__':
    with open('/home/hrichter/Downloads/coct.html', 'r') as f:
        lines = extract_coct_loadshedding_text(f.read())

    import pprint
    pprint.pprint(lines)
