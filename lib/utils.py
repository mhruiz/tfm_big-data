import re


def remove_accents(string: str) -> str:
    '''
    Remove accented characters from the given string.

    Parameters
    ----------
    string : str
        Original string.

    Returns
    -------
    str
        Unaccented string.
    '''
    return string.replace('á', 'a')\
                    .replace('é','e')\
                        .replace('í','i')\
                            .replace('ó','o')\
                                .replace('ú','u')


def normalize_string(string: str) -> str:
    '''
    Converts to lowercase, replaces white spaces with '_' and deletes punctuation characters.

    Parameters
    ----------
    string : str
        Original string.

    Returns
    -------
    str
        Normalized string.
    '''
    return remove_accents(re.sub(r'[^\w]', '', re.sub('\s+','_',string.lower())))


def get_numeric_characters(string: str) -> str:
    '''
    Extract numeric characters from string.

    Parameters
    ----------
    string : str
        Original string.

    Returns
    -------
    str
        Numeric characters.
    '''

    return ''.join(re.findall('\d', string))