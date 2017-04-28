# -*- coding: utf-8 -*-
import dpkt
import codecs


class NetworkAnalyzer:
    def __init__(self):
        pass

    def analysis_http(self, pcap_file):
        f = codecs.open(pcap_file, 'r', 'utf-8')
        pcap = dpkt.pcap.Reader(f)
        for ts, buf in pcap:
            eth = dpkt.ethernet.Ethernet(buf)
            ip = eth.data
            tcp = ip.data

            if tcp.dport == 80 and len(tcp.data) > 0:
                http = dpkt.http.Request(tcp.data)
                print([http.url, http.body, http.headers, http.method])

        f.close()

if __name__ == '__main__':
    analyzer = NetworkAnalyzer()
    analyzer.analysis_http(r'C:\Users\apple\Desktop\test.pcap')
