from urllib.request import urlopen
from urllib.error import HTTPError
from bs4 import BeautifulSoup as bs
from sqlalchemy import create_engine, select
from sqlalchemy import Column, Date, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import re
import os
import sys
from datetime import datetime

from loggeur import create_logger


def get_soup(source):
    try:
        page = urlopen(source).read()
        soup = bs(page, "html.parser")
        logger.info(f"Got soup for {source}")
        return soup
    except HTTPError:
        logger.error(f"Couldn't get to {source}")


def get_blog_page_record_href(soup):
    all_hrefs = []
    a_tags = soup.find_all("a", {"class": "title"})
    for link in a_tags:
        all_hrefs.append(link.attrs['href'])
    return all_hrefs


def get_all_tags(base_url):
    soup = get_soup(base_url + "1")
    tab1 = soup.find("div", {"id": "tab1"})
    ul = tab1.contents[3]
    return [a_tag.contents[0] for a_tag in ul.find_all("a")]


class Sources:

    def __init__(self, source_pattern):
        self.source_pattern = source_pattern
        self.current_page_number = 0
        self.total_page_number = self.get_total_page_number()

    def __iter__(self):
        return self

    def __next__(self):
        if self.current_page_number < self.total_page_number:
            self.current_page_number += 1
            logger.info(f"Blog page number: {self.current_page_number}/{self.total_page_number}")
            return self.source_pattern + str(self.current_page_number)
        logger.info("No more pages blog pages")
        raise StopIteration

    def get_total_page_number(self):
        soup = get_soup(self.source_pattern + "1")
        try:
            div_with_total_page_number = soup.find_all("div", {"class": "title"})
            div_with_total_page_number_content = div_with_total_page_number[0].contents[0]
            logger.info("Found tag with total page number")
        except Exception as e:
            logger.error("Didn't find tag with total page number ")
        try:
            extracted_numbers_from_div_content = re.findall(r"[0-9,.]+",
                                                            div_with_total_page_number_content)  # extract all numbers including those with comma
            total_page_number = int(
                extracted_numbers_from_div_content[-1].replace(",", ""))  # the total page number is the last number
            logger.info(f"Total page number: {total_page_number}")
            return total_page_number
        except Exception as e:
            logger.error("Didn't succeed to extract total page number")


class ScrappedRecords:
    def __init__(self, page_url):

        # parameter needed to init object
        self.page_url = page_url

        # parameters usefull to build object (will be removed after remove_unwanted_attributes() )
        self.main_div = None
        self.artist_and_record_name = None
        self.tags_and_date_field = None
        self.section = None

        # parameters to get to send to db
        self.artist_name = None
        self.record_name = None
        self.creation_date = None
        self.record_type = None
        self.tags = None
        self.label = None
        self.songs = None

        # actions to perform
        self.preprocess_page()
        self.set_attributes()
        self.remove_unwanted_attributes()

        # usefull dict ...
        self.__dict__ = vars(self)

    def preprocess_page(self):
        try:
            self.set_main_div()
            try:
                self.set_artist_and_record_name()
            except Exception as e:
                logger.error(f"   --> Couldn't get tags_and_date_field\n{e}")
            try:
                self.set_section()
                try:
                    self.set_tags_and_date_field()
                except Exception as e:
                    logger.error(f"   --> Couldn't get tags_and_date_field\n{e}")
            except Exception as e:
                logger.error(f"   --> Couldn't get section\n{e}")
        except Exception as e:
            logger.error(f"   --> Couldn't get main_div\n{e}")

    def set_attributes(self):

        if self.main_div:
            if self.artist_and_record_name:
                try:
                    self.set_artist_name()
                except Exception as e:
                    logger.error(f"   --> Specific problem with artist_name\n{e}")
                try:
                    self.set_record_name()
                except Exception as e:
                    logger.error(f"   --> Specific problem with record_name\n{e}")

            if self.section:
                try:
                    self.set_label()
                except Exception as e:
                    logger.error(f"   --> Specific problem with label\n{e}")

                try:
                    self.set_songs()
                except Exception as e:
                    logger.error(f"   --> Specific problem with songs\n{e}")

                if self.tags_and_date_field:
                    try:
                        self.set_creation_date()
                    except Exception as e:
                        logger.error(f"   --> Specific problem with creation_date\n{e}")

                    try:
                        self.set_tags()
                    except Exception as e:
                        logger.error(f"   --> Specific problem with tags\n{e}")

                    try:
                        self.set_record_type()
                    except Exception as e:
                        logger.error(f"   --> Specific problem with tags\n{e}")

    def remove_unwanted_attributes(self):
        del self.tags_and_date_field
        del self.section
        del self.page_url
        del self.main_div
        del self.artist_and_record_name

    def set_main_div(self):
        soup = get_soup(self.page_url)
        self.main_div = soup.find("div", {"id": "main"})

    def set_artist_and_record_name(self):
        self.artist_and_record_name = self.main_div.contents[1].contents[5].text.split(" / ", 1)

    def set_section(self):
        self.section = self.main_div.contents[3].contents[3].contents[1].contents[3].contents

    def set_tags_and_date_field(self):
        self.tags_and_date_field = self.section[1].contents

    def set_artist_name(self):
        self.artist_name = self.artist_and_record_name[0]

    def set_record_name(self):
        self.record_name = self.artist_and_record_name[1][:-7]  # [-7] to remove [date] in string

    def set_creation_date(self):
        creation_date_as_datetime = datetime.strptime(self.tags_and_date_field[3].contents[0], '%b %d, %Y')
        self.creation_date = datetime.strptime(creation_date_as_datetime.strftime('%Y-%m-%d'), '%Y-%m-%d')

    def set_tags(self):
        self.tags = self.filter_tag_elements(self.tags_and_date_field[5])

    def set_record_type(self):
        record_type = [i for i in self.tags if i in
                       ["Album", "Boxset", "Compilation", "DJ Mix", " Documentary", "DVD", "EP", "Mixtape", "Single"]]
        if record_type:
            self.record_type = record_type[0]

    def set_label(self):
        label_field = self.section[4].lstrip().rstrip()  # to remove \n and\t at the beginning and end of str
        self.label = label_field[len("[Label: "):label_field.rfind(" | ")]

    def set_songs(self):
        self.songs = self.filter_tag_elements(self.section[5])

    @staticmethod
    def filter_tag_elements(tag_to_filter):
        return [item.contents[0] for i, item in enumerate(tag_to_filter) if i % 2 == 1]


def preprocess_record_before_db_insertion(record_dict):
    """
    transform list to string in order to insert record parameters in database
    """

    for item in ["songs", "tags"]:
        item_as_string = ""
        for i, song in enumerate(record_dict[item]):
            item_as_string += song
            if i < len(record_dict[item]) - 1:
                item_as_string += ";"
        record_dict[item] = item_as_string
    return record_dict


def table_tag_creator(tablename):
    class Tag(Base):
        __tablename__ = tablename
        id = Column(Integer, primary_key=True)
        record_id = Column(Integer)
    return Tag


def record_tag_creator():
    class Record(Base):
        __tablename__ = "record"

        id = Column(Integer, primary_key=True)
        artist_name = Column(String)
        record_name = Column(String)
        record_type = Column(String)
        tags = Column(String)
        label = Column(String)
        songs = Column(String)
        creation_date = Column(Date)
    return Record


if __name__ == "__main__":

    # set usefull variables
    nodata_blog_url_pattern = "https://nodata.tv/blog/page/"  # the base pattern to scrap, add [1:1:1704] to have a page
    db_path = os.path.join(sys.path[0], "my_db.db")  # the path to db

    # Init logger
    logger = create_logger()

    # Init database
    Base = declarative_base()
    engine = create_engine('sqlite:///' + db_path, echo=False)

    # tables creation
    record_table = record_tag_creator()
    table_tag_dict = {}
    for tag in get_all_tags(nodata_blog_url_pattern):
        table_tag_dict[tag] = table_tag_creator(tag)

    # associate tables to db
    Base.metadata.create_all(engine)

    # create session
    Session = sessionmaker(bind=engine)
    session = Session()

    # loop over nodata blog pages
    for source in Sources(nodata_blog_url_pattern):

        # access html of current blog page
        soup = get_soup(source)
        if soup is not None:

            # get all accessible record pages from current blog page
            all_record_href_of_current_blog_page = get_blog_page_record_href(soup)

            for record_href in all_record_href_of_current_blog_page:
                logger.info(f" --> {record_href}")

                # process current record page to get all needed values for db
                record_as_dict = ScrappedRecords(record_href).__dict__
                current_record_tags = record_as_dict["tags"]

                if record_as_dict:

                    # replace list by string in record_as_dict
                    preprocess_record_before_db_insertion(record_as_dict)

                    # insert record in db
                    record = record_table(**record_as_dict)
                    session.add(record)

                    for tag in current_record_tags:
                        current_tag = table_tag_dict[tag]
                        session.add(current_tag)

                    session.commit()

                    # Just to check if record have been correctly inserted
                    stmt = select('').select_from(record_table)
                    result = session.execute(stmt).fetchall()
                    print(result)

                    # get id of last added record

                    # loop over current_record_tag to insert the id of last added record in each tag table



# TODO
#  - create dynamically table for each existing tag and store it in a dict {"tag": "TableName"}
#  - once last record insert, get its id to store it in records

