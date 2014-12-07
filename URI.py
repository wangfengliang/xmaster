#!/usr/bin/env python
#coding=utf-8

import sys
import urlparse

class URI(object):
    # 顶级域名列表(国际域名+国家域名)
    domain_lst = ["com", "edu", "gov", "int", "mil", "net", "org", "biz", "info", "pro", "name", "museum", "coop", "aero", "ac", "ad", "ae", "af", "ag", "ai", "al", "am", "an", "ao", "aq", "ar", "as", "at", "au", "aw", "az", "ba", "bb", "bd", "be", "bf", "bg", "bh", "bi", "bj", "bm", "bn", "bo", "br", "bs", "bt", "bv", "bw", "by", "bz", "ca", "cc", "cd", "cf", "cg", "ch", "ci", "ck", "cl", "cm", "cn", "co", "cr", "cu", "cv", "cx", "cy", "cz", "de", "dj", "dk", "dm", "do", "dz", "ec", "ee", "eg", "eh", "er", "es", "et", "fi", "fj", "fk", "fm", "fo", "fr", "ga", "gd", "ge", "gf", "gg", "gh", "gi", "gl", "gm", "gn", "gp", "gq", "gr", "gs", "gt", "gu", "gw", "gy", "hk", "hm", "hn", "hr", "ht", "hu", "id", "ie", "il", "im", "in", "io", "iq", "ir", "is", "it", "je", "jm", "jo", "jp", "ke", "kg", "kh", "ki", "km", "kn", "kp", "kr", "kw", "ky", "kz", "la", "lb", "lc", "li", "lk", "lr", "ls", "lt", "lu", "lv", "ly", "ma", "mc", "md", "mg", "mh", "mk", "ml", "mm", "mn", "mo", "mp", "mq", "mr", "ms", "mt", "mu", "mv", "mw", "mx", "my", "mz", "na", "nc", "ne", "nf", "ng", "ni", "nl", "no", "np", "nr", "nu", "nz", "om", "pa", "pe", "pf", "pg", "ph", "pk", "pl", "pm", "pn", "pr", "ps", "pt", "pw", "py", "qa", "re", "ro", "ru", "rw", "sa", "sb", "sc", "sd", "se", "sg", "sh", "si", "sj", "sk", "sl", "sm", "sn", "so", "sr", "st", "sv", "sy", "sz", "tc", "td", "tf", "tg", "th", "tj", "tk", "tl", "tm", "tn", "to", "tp", "tr", "tt", "tv", "tw", "tz", "ua", "ug", "uk", "um", "us", "uy", "uz", "va", "vc", "ve", "vg", "vi", "vn", "vu", "wf", "ws", "ye", "yt", "yu", "za", "zm", "zw"]
    
    @staticmethod
    def hostname(url):
        return urlparse.urlsplit(url).hostname
    
    @staticmethod
    def domain(host):
        l = host.split('.')
        for i,v in enumerate(l[::-1]): # 逆序
            if v not in URI.domain_lst:
                i = len(l) - i - 1
                domain_ = '.'.join(l[i:])
                return domain_
        return host
    
    @staticmethod
    def domain2(url):
        url_hostname = URI.hostname(url)
        return URI.domain(url_hostname)
    
    # 返回各级域名
    @staticmethod
    def domains(url):
        _domains = []
        url_hostname = URI.hostname(url)
        l = url_hostname.split('.')
        for i,v in enumerate(l[::-1]): # 逆序
            if v not in URI.domain_lst:
                i = len(l) - i - 1
                _domain = '.'.join(l[i:])
                _domains.append(_domain)
        return _domains[::-1]
    

if __name__ == "__main__":

    url = "http://www.baidu.com/"
    print URI.domains(url)

    sys.exit(1)

    hosts = ["fashion.haibao.com", "street.yoka.com", "www.vogue.com.cn" ]
    for host in hosts:
        domain = domain(host)
        print "%s -> %s" % (host, domain)

