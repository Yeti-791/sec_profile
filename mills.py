# -*- coding: utf-8 -*-
import codecs
import datetime
import hashlib
import logging
import os
import re
import sqlite3
from urlparse import urlparse

import requests
import tldextract
from bs4 import BeautifulSoup


def list2str(l):
    """
    列表转字符串
    :param l:
    :return:
    """
    st_list = []
    for i in l:
        item = "\t".join(i)
        st_list.append(item)
    return os.linesep.join(st_list)


def path(*paths):
    """
    :param paths:
    :return:
    """
    MODULE_PATH = os.path.dirname(os.path.realpath(__file__))
    # ROOT_PATH = os.path.join(MODULE_PATH, os.path.pardir)
    return os.path.abspath(os.path.join(MODULE_PATH, *paths))


def parse_url(url, isupdate=False):
    """
    url解析
    :param url:
    :return:
    """
    o = urlparse(url)
    extract = tldextract.TLDExtract()
    if isupdate:
        extract.update()
    ext = extract(url)
    # o.schema, o.netloc,o.path
    # ext.subdomain, ext.suffix, ext.domain + "." + ext.suffix
    return [o, ext]


def strip_n(st):
    """

    :param st:
    :return:
    """
    st = re.sub(r'\n', ' ', st)
    st = re.sub(r'\s+', ' ', st)
    st = st.strip()
    return st


class SQLiteOper(object):
    """

    """

    def __init__(self, dbpath="", db_is_new=False, schemafile=""):
        if db_is_new:
            print "create new schema"
            if os.path.exists(dbpath):
                os.remove(dbpath)
            with codecs.open(schemafile, mode='rb', encoding='utf-8', errors='ignore') as f:
                schema = f.read()
                self.sqlite3_conn = sqlite3.connect(dbpath, timeout=20)
                self.executescript(schema)
        self.sqlite3_conn = sqlite3.connect(dbpath, timeout=20)

    def __del__(self):
        self.sqlite3_conn.close()

    def executescript(self, sql_script):
        self.sqlite3_conn.executescript(sql_script)

    def query(self, query_statement, operate_dict=None):
        """
        select statement
        :param query:
        :return:
        """

        logging.debug(query_statement)
        cursor = self.sqlite3_conn.cursor()
        if operate_dict is not None:
            cursor.execute(query_statement, operate_dict)
        else:
            cursor.execute(query_statement)
        # row_count = len(cursor.fetchall())
        # if row_count != 0:
        #    print "%s %d" % (query_statement, row_count)
        for line in cursor.fetchall():
            yield line

    def executemany(self, operate_statement, operate_list=None):
        """insert/update"""
        # print insert_statement
        logging.debug(operate_statement)
        cursor = self.sqlite3_conn.cursor()

        cursor.executemany(operate_statement, operate_list)

        self.sqlite3_conn.commit()

    def execute(self, sql):
        """

        :param sql:
        :return:
        """
        logging.debug(sql)
        cursor = self.sqlite3_conn.cursor()

        cursor.execute(sql)

        self.sqlite3_conn.commit()


def get_special_date(delta=0, format="%Y%m%d"):
    """
    now 20160918, default delata = 0
    :return:
    """
    date = (datetime.date.today() + datetime.timedelta(days=delta)).strftime(format)
    return date


def get_weixin_info(url="", max_redirects=30, proxy=None, root_dir="data/weixin", retry=3, timeout=10):
    """
    微信解析
    :param url:
    :param max_redirects:
    :param proxy:
    :param root_dir:
    :return:
    """
    root_dir = path(root_dir)
    if not os.path.exists(root_dir):
        os.mkdir(root_dir)
    hl = hashlib.md5()
    hl.update(url.encode(encoding='utf-8'))

    fname = path(root_dir, "%s.html" % hl.hexdigest())

    if not os.path.exists(fname):
        get_request(url, proxy=proxy, fname=fname, retry=retry, max_redirects=max_redirects, timeout=timeout)

    if os.path.exists(fname):
        with codecs.open(fname, mode='rb') as fr:
            try:
                soup = BeautifulSoup(fr, 'lxml')

            except Exception as e:
                logging.error("GET title of %s failed : %s" % (url, repr(e)))
                return

            title = soup.find('title')

            rich_media_meta_list = soup.find("div", class_="rich_media_meta_list")

            nickname_chineses = ""
            nickname_english = ""
            weixin_no = ""
            weixin_subject = ""

            if rich_media_meta_list:
                media_meta_text = rich_media_meta_list.find("span", class_="rich_media_meta rich_media_meta_text")

                if media_meta_text:
                    nickname_chineses = media_meta_text.text

                profile_inner = rich_media_meta_list.find("div", class_="profile_inner")

                if profile_inner:
                    weixin_no = profile_inner.find("strong", class_="profile_nickname")
                    if weixin_no:
                        nickname_english = weixin_no.text

                    profile_metas = profile_inner.find_all("span", class_="profile_meta_value")

                    if profile_metas:
                        if len(profile_metas) == 2:
                            weixin_no = profile_metas[0].text
                            weixin_subject = profile_metas[1].text

            if title:
                title = title.get_text()
            else:
                title = ""

            return strip_n(nickname_english), url, \
                   strip_n(nickname_chineses), \
                   strip_n(weixin_no), \
                   strip_n(weixin_subject), \
                   strip_n(title)


def get_github_info(url="", max_redirects=30, proxy=None, root_dir="data/githubcom", isnew=False,
                    retry=3, timeout=10):
    """
    github解析
    :param url:
    :param max_redirects:
    :param proxy:
    :param root_dir:
    :return:
    """
    file_404 = path("data/github_404")
    urls_404 = set()
    if os.path.exists(file_404):

        with codecs.open(file_404, mode='rb') as fr:
            for line in fr:
                line = line.strip()
                if line:
                    urls_404.add(line)

    pattern = "(https://github.com/([^/]+))"
    match = re.search(pattern, url)
    if match:
        url_root, github_id = match.groups()
        if url_root in urls_404:
            return
    else:
        return

    root_dir = path(root_dir)
    if not os.path.exists(root_dir):
        os.mkdir(root_dir)

    hl = hashlib.md5()
    hl.update(url.encode(encoding='utf-8'))

    fname = path(root_dir, "%s.html" % hl.hexdigest())

    if isnew or not os.path.exists(fname):
        get_request(url, proxy=proxy, fname=fname, fname_404=file_404, retry=retry,
                    timeout=timeout, max_redirects=max_redirects)

    if os.path.exists(fname):
        with codecs.open(fname, mode='rb') as fr:
            try:
                soup = BeautifulSoup(fr, 'lxml')

            except Exception as e:
                logging.error("GET title of %s failed : %s" % (url, repr(e)))
                return

            title = soup.find("title")
            if title:

                title = title.get_text()
                title = strip_n(title)

            else:
                title = 'None'
            # find org-description
            org_sub = soup.find("p", class_='org-description')
            if org_sub:
                org_sub = org_sub.next_sibling
                if org_sub:
                    org_sub = org_sub.get_text()
                    org_sub = strip_n(org_sub)

            org = soup.find("ul", class_=re.compile("org-header-meta"))
            person = None
            if org:
                org = org.get_text()
                org = strip_n(org)



            else:

                person = soup.find('div', class_="js-profile-editable-area")
                if person:
                    person = person.get_text()
                    person = strip_n(person)

            if github_id is None:
                return
            if url_root is None:
                return
            if url is None:
                return

            if title is None:
                title = "None"
            if org is None:
                org = "None"
            if org_sub is None:
                org_sub = "None"
            if person is None:
                person = "None"

            return github_id, url_root, org_sub, org, person, url, title


def get_github_org(fname):
    """
    github分类：组织或私人
    :param fname:
    :return:
    """
    fname_org = "%s_org" % fname
    fname_private = "%s_private" % fname
    if os.path.exists(fname):
        with codecs.open(fname_org, mode='wb') as fw_org, \
                codecs.open(fname_private, mode='wb') as fw_private, \
                codecs.open(fname, mode='rb') as fr:
            for line in fr:
                line = line.strip()
                if line:
                    parts = re.split('\s+', line, 4)
                    if len(parts) < 4:
                        continue
                    org_sub = parts[3]

                    if org_sub != 'None':
                        fw_org.write("%s%s" % (line, os.linesep))
                    else:
                        fw_private.write("%s%s" % (line, os.linesep))
        os.unlink(fname)


def get_request(url, max_redirects=30, proxy=None, fname=None, fname_404=None, retry=3, timeout=10):
    """
    请求
    :param url:
    :param max_redirects:
    :param proxy:
    :param fname:
    :param fname_404:
    :param retry:
    :param timeout:
    :return:
    """

    ret = False
    headers = {}
    headers['User-Agent'] = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 " \
                            "(KHTML, like Gecko) Chrome/52.0.2743.82 Safari/537.36"

    s = requests.session()
    s.max_redirects = max_redirects

    s.proxies = proxy

    while retry > 0:
        try:

            r = s.request(url=url,
                          method="GET",
                          headers=headers,
                          timeout=timeout

                          )

            if r.reason == "OK":

                with codecs.open(fname, mode='wb') as fw:
                    ret = True
                    fw.write(r.content)
                    retry = 0
            elif r.reason == "Not Found":
                with codecs.open(fname_404, mode='ab') as fw:
                    ret = False
                    fw.write("%s%s" % (url, os.linesep))
                    retry = 0

            else:

                logging.warning("[url]: retry:%d %s, %s" % (retry, url, r.reason))
                retry = retry - 1

        except Exception as e:
            logging.error("[REQUEST]: retry:%d %s %s" % (retry, url, str(e)))

            retry = retry - 1

    return ret


def get_twitter_info(url, proxy=None, root_dir="data/twitter", isnew=False, retry=1, timeout=10):
    """
    twitter解析
    :param short_url:
    :return:
    """

    file_404 = path("data/twitter_404")
    urls_404 = set()
    if os.path.exists(file_404):

        with codecs.open(file_404, mode='rb') as fr:
            for line in fr:
                line = line.strip()
                if line:
                    urls_404.add(line)

    if urls_404 and url in urls_404:
        return

    root_dir = path(root_dir)
    if not os.path.exists(root_dir):
        os.mkdir(root_dir)

    hl = hashlib.md5()
    hl.update(url.encode(encoding='utf-8'))

    fname = path(root_dir, "%s.html" % hl.hexdigest())

    if isnew or not os.path.exists(fname):
        get_request(url, proxy=proxy, fname=fname, fname_404=file_404, retry=retry, timeout=timeout)

    if os.path.exists(fname):
        with codecs.open(fname, mode='rb') as fr:
            try:
                soup = BeautifulSoup(fr, 'lxml')

            except Exception as e:
                logging.error("GET title of %s failed : %s" % (url, repr(e)))
                return

            title = soup.find("title")

            twitter_account = soup.find("a", class_=re.compile(r'account-group js-account-group'))

            profile_header = soup.find("p", class_=re.compile(r'ProfileHeaderCard-bio'))

            if profile_header:
                profile_header = profile_header.get_text()
            else:
                profile_header = "None"

            if twitter_account:
                twitter_account = twitter_account.get("href")
            else:
                twitter_account = "None"

            if title:

                title = title.get_text()
                title = strip_n(title)
                if title.startswith("https://twitter.com"):
                    details = get_twitter_info(title, proxy=proxy, isnew=isnew)
                    if details:
                        return details



            else:
                title = 'None'

            return strip_n(twitter_account), url, strip_n(profile_header), strip_n(title)


def get_redirect_url(url, proxy=None, root_dir="data/shorturl", isnew=False, retry=1, timeout=10):
    """

    :param urls:
    :return:
    """

    file_404 = path("data/shorturl_404")
    urls_404 = set()
    if os.path.exists(file_404):

        with codecs.open(file_404, mode='rb') as fr:
            for line in fr:
                line = line.strip()
                if line:
                    urls_404.add(line)

    if urls_404 and url in urls_404:
        return

    root_dir = path(root_dir)
    if not os.path.exists(root_dir):
        os.mkdir(root_dir)

    hl = hashlib.md5()
    hl.update(url.encode(encoding='utf-8'))

    fname = path(root_dir, "%s.html" % hl.hexdigest())

    if isnew or not os.path.exists(fname):
        get_request(url, proxy=proxy, retry=retry, timeout=timeout, fname=fname, fname_404=file_404)

    if os.path.exists(fname):
        with codecs.open(fname, mode='rb') as fr:
            try:
                soup = BeautifulSoup(fr, 'lxml')

            except Exception as e:
                logging.error("GET title of %s failed : %s" % (url, repr(e)))
                return

            title = soup.find("title")
            if title:
                title = title.get_text()
                if title.startswith("http"):
                    # title is the real url
                    o, ext = parse_url(title)
                    domain = o.netloc
                    url_path = o.path
                    root_domain = ext.domain + "." + ext.suffix

                    sql = "update xuanwu_detail " \
                          "set url='{title}',root_domain='{root_domain}', domain='{domain}',path='{path}' " \
                          "where url='{url}'; ".format(
                        root_domain=root_domain,
                        domain=domain,
                        path=url_path,
                        url=url,
                        title=title

                    )
                    return sql


if __name__ == "__main__":
    """
    """

    proxy = {
        'http': 'http://xxx',
        "https": 'https://xxx'
    }

    url = "https://t.co/BykA8x8NkQ"
    url = "https://twitter.com/i/web/status/842316798057971712"
    url = "https://github.com/tanjiti"
    url = "http://bit.ly/29fuX1k"
    # url = "http://ow.ly/K5pl301Ok5U"

    ret = get_redirect_url(url, proxy=proxy, isnew=False, retry=3, timeout=10)
    if ret:
        print ret
