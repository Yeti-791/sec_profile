# -*- coding: utf-8 -*-
import sys

reload(sys)
sys.setdefaultencoding('utf8')
import matplotlib.pyplot as plt

from statistic import info_source
from mills import SQLiteOper



def draw_pie(so,source="secwiki",year="",tag="domain",top=10):
    """

    :return:
    """


    ods = info_source(so, table="{source}_detail".format(source=source),
                      top=top,
                      year=str(year),
                      tag=tag)


    labels = []
    values = []
    for k, v in ods.items():
        labels.append(k)
        values.append(v)
    labels.append("other")
    values.append(1-sum(values))

    explode = [0 for _ in range(0,top+1)]


    plt.rcParams['font.sans-serif'] = ['SimHei']  # 解决中文乱码
    plt.pie(values,
            explode=explode,
            labels=labels,
            labeldistance = 1.2,
            pctdistance=0.6,
            startangle=90,
            shadow=False,
            autopct = '%3.2f%%')
    plt.title('信息源')
    plt.show()


if __name__ == "__main__":
    """
    """
    so = SQLiteOper("data/scrap.db")
    draw_pie(so,source="secwiki",year="2014",tag="domain",top=10)