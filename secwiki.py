# -*- coding: utf-8 -*-
import sys

reload(sys)
sys.setdefaultencoding('utf8')
import logging
import glob
import re
import os
import codecs

import requests
from bs4 import BeautifulSoup

from mills import parse_url
from mills import SQLiteOper
from mills import path


def scrap_item(i=1):
    """
    爬取单个页面
    :return:
    """
    url = "https://www.sec-wiki.com/weekly/{i}".format(i=i)
    fname = path("data/secwiki/{i}_week.html".format(i=i))
    logging.info("[SCRAP_PAGE]: %s" % url)
    try:

        r = requests.get(url)
        if r.status_code == 200:
            with codecs.open(fname, mode='wb') as fw:
                fw.write(r.content)
                return fname

    except Exception as e:
        logging.error("[SCRAP_REQUEST_FAILED]: %s %s" % (url, str(e)))


def scrap_all(start, end):
    """
    爬取指定页面
    :param start:
    :param end:
    :return:
    """

    fnames = []
    for i in range(start, end):
        fname = scrap_item(i)
        fnames.append(fname)
    return fnames


def sort_fname(fnames):
    """
    文件名排序
    :return:
    """
    nos = {}
    for fname in fnames:
        m = re.search(r'(\d+)_week\.html', fname)
        if m:
            nos[int(m.group(1))] = fname

    return nos


def scrap_latest():
    """
    爬取最新页面
    :return:
    """
    cur_no = -1
    url = "https://www.sec-wiki.com/weekly"
    try:
        r = requests.get(url)
        if r.status_code == 200:
            html_content = r.content
            soup = BeautifulSoup(html_content, "lxml")

            weekly_string = soup.find("div", class_="issues").a["href"][8:]
            cur_no = int(weekly_string)


    except Exception as e:
        logging.error("[REQUEST]: %s %s" % (url, str(e)))

    nos = sort_fname(glob.glob("data/secwiki/*.html"))
    last_no = max(nos.keys())
    if cur_no > 0:

        if cur_no > last_no:
            fnames = scrap_all(last_no + 1, cur_no + 1)
            return fnames


def parse_item(html_hd):
    """
    解析单个页面
    :param page:
    :return:
    """

    soup = BeautifulSoup(html_hd, "lxml")
    # find_day
    #  2014/03/03-2014/03/09
    day = soup.find("blockquote").text
    p = re.compile(r'(\d{4})\/(\d{2})\/(\d{2})')
    m = re.search(p, day)
    if m:
        ts = m.group(1) + m.group(2) + m.group(3)
    else:
        return

    page = soup.find(id="content")

    for div in page.find_all("div", class_='single'):

        sts = div.stripped_strings
        tag = sts.next()
        if tag.find("[") != -1:
            tag = tag[1:-1]

        title = sts.next()

        # url

        for url in div.find_all("a"):
            url = url["href"]
            o, ext = parse_url(url)
            domain = o.netloc
            url_path = o.path
            root_domain = ext.domain + "." + ext.suffix
            result = (ts, tag, url, title, root_domain, domain, url_path)

            yield result


def parse_all(fnames=None, renew=False):
    """
    批量解析页面
    :param fnames:
    :param renew 是否重新解析所有文件
    :return:
    """
    so = SQLiteOper("data/scrap.db")
    if renew:
        fnames = []
        fname_gen = glob.iglob(r'data/secwiki/*.html')
        sql = 'delete from `secwiki_detail`'
        for f in fname_gen:
            fnames.append(f)

        so.execute(sql)

    if fnames is None:
        print "no new secwiki"
        return

    nos = sort_fname(fnames)

    # sqlite handler
    sql = """insert into `secwiki_detail`(`ts`,`tag`,`url`,`title`,`root_domain`,`domain`,`path`)
                            values(?,?,?,?,?,?,?);"""

    # file handler

    result_fname = path("data/secwiki_{start}_{end}.txt".format(
        start=nos.keys()[0],
        end=nos.keys()[-1]
    ))

    if not renew and os.path.isfile(result_fname) and os.path.getsize(result_fname) > 0:
        return

    result_fh = codecs.open(result_fname, mode='wb')
    for k in nos.keys():
        fname = nos[k]

        with open(fname, mode='r') as html_hd:
            results_list = {}
            for content in parse_item(html_hd):
                if content:
                    k = content[0] + content[2]

                    results_list[k] = content

                    line = "\t".join(content)
                    print line
                    result_fh.write("{line}{linesep}".format(line=line, linesep=os.linesep))

            so.executemany(sql, operate_list=results_list.values())

    result_fh.close()


def main():
    """

    :return:
    """
    filenames = scrap_latest()

    parse_all(filenames, renew=False)


if __name__ == "__main__":
    """
    """
    main()
