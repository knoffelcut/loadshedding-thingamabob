class ScrapeError(ValueError):
    pass

def soup_me(soup: 'bs4.BeautifulSoup'):
    """Converts soup to a beautifulsoup4 object

    Args:
        soup (bs4.BeautifulSoup or markup): input
    """
    import bs4

    if not isinstance(soup, bs4.BeautifulSoup):
        # Use 'lxml' instead of 'html.parser', since the CoCT website,
        # specifically the section relating to the schedule,
        # contains invalid </br> tags, which are just supposed to be <br>. It is the only place <br> tags are used.. ATM
        #  Similar to this issue https://stackoverflow.com/a/44935244
        soup = bs4.BeautifulSoup(soup, 'lxml')

    return soup

def extract_coct_loadshedding_text(soup: 'bs4.BeautifulSoup'):
    """Extracts the text relevant to the current loadshedding schedule from the CoCT website

    Args:
        soup (bs4.BeautifulSoup or markup): CoCT HTML
    """
    import bs4

    try:
        soup = soup_me(soup)

        soup = soup.find_all("p", class_="lrg")[0]
        lines = [line.strip() for line in soup.get_text().splitlines()]
        lines = [line for line in lines if line != '']

        return lines
    except Exception as e:
        raise ScrapeError from e

def extract_eskom_loadshedding_stage(response_text: str):
    """Extracts the current Eskom loadshedding stage

    Args:
        response_text (str): Eskom API response

    Returns:
        int: Current national loadshedding stage
    """
    try:
        stage = int(response_text)
        stage = stage - 1
        return stage
    except Exception as e:
        raise ScrapeError from e


if __name__ == '__main__':
    with open('/home/hrichter/Downloads/coct.html', 'r') as f:
        lines = extract_coct_loadshedding_text(f.read())

    import pprint
    pprint.pprint(lines)
