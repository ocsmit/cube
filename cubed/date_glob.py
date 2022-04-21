import re
import datetime


# ISO 8601
class date_glob:
    def __init__(self, datestr) -> None:
        self.datestr = datestr
        self.__Y = "[0-9]" * 4
        self.__m = "[0-9]" * 2
        self.__d = "[0-9]" * 2
        self.__j = "[0-9]" * 3
        self.pattern = self.__parse_date()

    def __parse_date(self):
        self.__pattern = self.datestr
        if "%Y" in self.datestr:
            self.__replace_year()
        if "%m" in self.__pattern:
            self.__replace_month()
        if "%d" in self.__pattern:
            print(self.__pattern)
            self.__replace_day()
        return self.__pattern

    def __replace_year(self):
        self.__pattern = self.__pattern.replace("%Y", self.__Y)

    def __replace_month(self):
        self.__pattern = self.__pattern.replace("%m", self.__m)

    def __replace_day(self):
        self.__pattern = self.__pattern.replace("%d", self.__d)
