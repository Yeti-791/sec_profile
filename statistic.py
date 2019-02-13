# -*- coding: utf-8 -*-
import sys

reload(sys)
sys.setdefaultencoding('utf8')
import codecs
import os
from collections import OrderedDict
import re

from mills import SQLiteOper
from mills import get_weixin_info
from mills import path
from mills import get_github_info
from mills import get_github_org
from mills import get_twitter_info


def info_source(so, table="secwiki_detail", year="", top=100, tag="domain"):
    """
    按tag或domain分组
    :param so:  sqlite3db
    :param table:
    :param year:
    :param top:
    :param tag: domain 或者 tag
    :return:
    """
    od = OrderedDict()
    sql = 'select {tag},count(path) as c ' \
          'from {table} ' \
          'where ts like "%{year}%" ' \
          'group by {tag} ' \
          'order by c desc ' \
          'limit 0,{top}'.format(table=table, year=year, top=top, tag=tag)
    result = so.query(sql)
    for item in result:
        od[item[0]] = item[1]

    od_perct = OrderedDict()
    sum_count = sum(od.values())
    for k, v in od.items():
        """
        """
        od_perct[k] = round(float(v) / sum_count, 4)
    return od_perct


def get_tag_domain_topn(so):
    """

    :param so:
    :return:
    """
    fname = path("data/tag_domain_top.txt")
    with codecs.open(fname, mode='wb') as fw:
        for tag in ["domain", "tag"]:
            for source in ["secwiki", "xuanwu"]:
                for year in [2014, 2015, 2016, 2017, 2018, '']:

                    ods = info_source(so, table="{source}_detail".format(source=source), top=61, year=str(year),
                                      tag=tag)
                    s = "############## %s %s " % (year, source)
                    print s
                    fw.write("%s%s" % (s, os.linesep))
                    for k, v in ods.items():
                        fw.write("%s\t%s%s" % (k, v, os.linesep))
                        print k, v


def get_network_id(so, source="weixin", renew=False, proxy=None, retry=3, timeout=10):
    """

    :param so:
    :param source:
    :param renew:
    :return:
    """

    # get urls
    urls = set()

    if source == "weixin":

        keyword = "://mp.weixin.qq.com/"
    elif source == "github.com":
        keyword = "https://github.com/"
    elif source == "zhihu.com":
        keyword = "zhihu.com"
    elif source == "twitter":
        keyword = "twitter"
    else:
        return
    for info_source in ["secwiki", "xuanwu"]:
        if source != "twitter":
            sql = "select distinct url from {source}_detail where url like '%{keyword}%'".format(
                keyword=keyword,
                source=info_source
            )
        else:
            sql = "select distinct url from xuanwu_detail where root_domain ='t.co' " \
                  "or root_domain='twitter.com'  "

        result = so.query(sql)
        for item in result:
            item = item[0]
            pos = item.find('http', 2)
            if pos != -1:
                urls_1 = item[0:pos]
                urls_2 = item[pos:]
                if urls_1.find(keyword) != -1:
                    urls.add(urls_1)
                if urls_2.find(keyword) != -1:
                    urls.add(urls_2)

            else:
                urls.add(item)

    fname = path("data", "%s.txt" % source)

    statisitc = {}

    if renew or not os.path.exists(fname):
        with codecs.open(fname, mode='wb', encoding='utf-8') as fw:
            for url in urls:

                if source == "weixin":
                    details = get_weixin_info(url)



                elif source == "github.com":

                    details = get_github_info(url)


                elif source == "twitter":

                    details = get_twitter_info(url, proxy=proxy, retry=retry, timeout=timeout)



                else:
                    details = ""
                    print url

                if details:
                    st = "\t".join(details)
                    print st
                    fw.write("{st}{linesep}".format(st=st, linesep=os.linesep))

    # statitic

    with codecs.open(fname, mode='rb', encoding='utf-8') as fr:

        for line in fr:

            line = line.strip()

            if line:

                parts = re.split(r'\t', line, 1)
                if len(parts) != 2:
                    continue
                k, v = parts
                if k in statisitc:

                    statisitc[k].append(v)
                else:
                    statisitc[k] = [v]

    fname_sort = path("data", "%s_sort.txt" % source)
    if statisitc:
        sorted_statistic = OrderedDict(sorted(statisitc.items(), key=lambda x: len(x[1]), reverse=True))
        with codecs.open(fname_sort, mode='wb', encoding='utf-8') as fw:
            for k, v in sorted_statistic.items():
                if k != "None":
                    fw.write("%s %d %s %s" % (k, len(v), v[0], os.linesep))
                    print k, len(v), v[0]

    if source == "github.com":
        get_github_org(fname_sort)


if __name__ == "__main__":
    """
    """
    proxy = {
        'http': 'http://xxx',
        "https": 'https://xxx'
    }

    so = SQLiteOper("data/scrap.db")

    get_tag_domain_topn(so)
    for source in ['weixin', 'github.com', 'twitter']:
        get_network_id(so, source=source, renew=False)
