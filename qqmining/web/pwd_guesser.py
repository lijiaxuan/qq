# -*- coding: utf-8 -*-

import os
from pypinyin import lazy_pinyin
import random

"""
A terribly naive implementation of Personal-PCFG
"""


def convert_grammar(grammar):
    """
    Convert grammar to short grammar
    :param grammar:
    :return:
    """
    prev_char = ''
    char_tag = 0
    short_grammar = []
    for ch in grammar:
        if ch == prev_char:
            char_tag += 1
        else:
            if char_tag != 0:
                short_grammar.append(prev_char + str(char_tag))
            char_tag = 1
            prev_char = ch
    if char_tag != 0:
        short_grammar.append(prev_char + str(char_tag))
    return short_grammar


def list_all_files(file_path):
    return [os.path.join(file_path, f) for f in os.listdir(file_path) if os.path.isfile(os.path.join(file_path, f))]


def find_last(string, str):
    last_position = -1
    while True:
        position = string.find(str, last_position + 1)
        if position == -1:
            return last_position
        last_position = position
    return last_position


def get_candidates_1(query):
    """
    Get candidates with length 1
    :param query:
    :return:
    """
    candidates = list()
    surname = lazy_pinyin(query)[0]
    candidates.append(surname)
    candidates.append(surname[0])
    return candidates


def get_candidates_2(query):
    """
    Get candidates with length 2
    :param name:
    :return:
    """
    candidates = list()
    tmp = lazy_pinyin(query)
    firstname = tmp[0]
    lastname = tmp[1]
    # liangxiao
    candidates.append(firstname + lastname)
    # xiaoliang
    candidates.append(lastname + firstname)
    # xliang
    candidates.append(lastname[0] + firstname)
    # liang
    candidates.append(firstname)
    # xiao
    candidates.append(lastname)
    # lx
    candidates.append(firstname[0] + lastname[0])
    # xl
    candidates.append(lastname[0] + firstname[0])
    # x
    candidates.append(lastname[0])
    # l
    candidates.append(firstname[0])
    return candidates


def get_candidates_3(query):
    """
    Get candidates with length 3
    :param query:
    :return:
    """
    # quxiaoyun
    candidates = list()
    whole = lazy_pinyin(query)
    surname = whole[0]
    midname = whole[1]
    lastname = whole[2]
    candidates.append("".join(whole))
    # xiaoyunqu
    candidates.append(midname + lastname + surname)
    # xyqu
    candidates.append(midname[0] + lastname[0] + surname)
    # qxy
    candidates.append(surname[0] + midname[0] + lastname[0])
    # xyq
    candidates.append(midname[0] + lastname[0] + surname[0])
    # xiaoyun
    candidates.append(midname + lastname)
    # qu
    candidates.append(surname)
    candidates.append(lastname)
    candidates.append(midname)
    return candidates


def get_candidates_4(query):
    """
    get candidates for length 4
    :param query:
    :return:
    """
    candidates = list()
    # ouyangxuetong
    whole = lazy_pinyin(query)
    candidates.append("".join(whole))
    name1 = whole[0]
    name2 = whole[1]
    name3 = whole[2]
    name4 = whole[3]
    # oyxt
    candidates.append(name1[0] + name2[0] + name3[0] + name4[0])
    # ou
    candidates.append(name1)
    # yang
    candidates.append(name2)
    # xue
    candidates.append(name3)
    # tong
    candidates.append(name4)
    # xt
    candidates.append(name3[0] + name4[0])
    # xuetong
    candidates.append(name3 + name4)
    # ouyang
    candidates.append(name1 + name2)
    # tongxue
    candidates.append(name4 + name3)
    candidates.append(name2[0] + name3[0] + name4[0])
    candidates.append(name1[0] + name2[0])
    candidates.append(name1 + name2[0])
    return candidates


def gen_name_candidates(name):
    """
    generate name candidates
    :param name:
    :return:
    """
    name_len = len(name)
    candidates = list()
    if name_len == 1:
        candidates = get_candidates_1(name)
    elif name_len == 2:
        # liangxiao
        candidates = get_candidates_2(name)
    elif name_len == 3:
        candidates = get_candidates_3(name)
    else:
        candidates = get_candidates_4(name)
    return candidates


def gen_birth_candidates(birth):
    """
    generate birth candidates
    :param birth:
    :return:
    """
    assert len(birth) == 8
    birth_candidates = list()
    # 19920818
    birth_candidates.append(birth)
    # 920818
    birth_candidates.append(birth[2:])
    # 1992
    birth_candidates.append(birth[:4])
    # 0818
    birth_candidates.append(birth[4:])
    # 1808
    birth_candidates.append(birth[6:] + birth[4:6])
    # 08
    birth_candidates.append(birth[4:6])
    birth_candidates.append(birth[6:])
    return birth_candidates


class NaivePCFGGuesser:
    def __init__(self, data_dir, dic_file_path):
        if not os.path.exists(data_dir):
            print("Data dir doesn't exist, exit...")
            return
        grammar_dir = os.path.join(data_dir, 'grammar')
        grammar_file_path = os.path.join(grammar_dir, 'structures.txt')
        self.grammar_list = self.parse_grammar(grammar_file_path)
        if not os.path.exists(dic_file_path):
            print('Check the dictionary file path...')
            return
        self.letter_dict = self.parse_dict(dic_file_path)
        digit_file_dir = os.path.join(data_dir, 'digits')
        self.digit_dict = self.parse_digit(digit_file_dir)
        special_file_dir = os.path.join(data_dir, 'special')
        self.special_dict = self.parse_special(special_file_dir)

    def parse_digit(self, digit_file_dir):
        """
        Parse digits
        :param digit_file_dir:
        :return:
        """
        digit_dict = dict()
        files = list_all_files(digit_file_dir)
        for file in files:
            for line in open(file):
                pieces = line.strip().split('\t')
                letters = pieces[0]
                letter_prob = float(pieces[1])
                key = 'D' + str(len(letters))
                if key not in digit_dict:
                    digit_dict[key] = list()
                digit_dict[key].append((letters, letter_prob))
        return digit_dict

    def parse_special(self, special_file_dir):
        """
        Parse special chars
        :param special_file_dir:
        :return:
        """
        special_dict = dict()
        files = list_all_files(special_file_dir)
        for file in files:
            for line in open(file):
                pieces = line.rstrip().split('\t')
                specials = pieces[0]
                special_prob = float(pieces[1])
                key = 'S' + str(len(specials))
                if key not in special_dict:
                    special_dict[key] = list()
                special_dict[key].append((specials, special_prob))
        return special_dict

    def parse_dict(self, dic_file_path):
        """
        Parse dictionary
        :param dic_file_path:
        :return:
        """
        letter_dict = dict()
        for line in open(dic_file_path):
            words = line.strip()
            key = 'L' + str(len(words))
            if key not in letter_dict:
                letter_dict[key] = list()
            letter_dict[key].append(words)
        return letter_dict

    def parse_grammar(self, grammar_file_path):
        """
        Parse grammar and generate list
        :param grammar_file_path:
        :return:
        """
        grammar_list = list()
        for line in open(grammar_file_path):
            pieces = line.strip().split()
            grammar = pieces[0].strip()
            short_grammar = convert_grammar(grammar)
            prob = float(pieces[1])
            grammar_list.append((short_grammar, prob))
        return grammar_list

    def gen_guesses(self, account=None, birth=None, name=None, email=None, cellphone=None, id=None, count=20):
        """
        Generate guesses according to personal information
        :param account:
        :param birth:
        :param name:
        :param email:
        :param cellphone:
        :param id:
        :return:
        """
        name_candidates = gen_name_candidates(name)
        birth_candidates = gen_birth_candidates(birth)
        gen_tag = 0
        email_prefix = email[:find_last(email, '@')]
        print(email_prefix)
        final_pwds = list()
        for short_grammar, prob in self.grammar_list:
            cur_pwd = ''
            ok = True
            for seg in short_grammar:
                seg_cat = seg[0]
                seg_len = int(seg[1:])
                if seg_cat == 'L':
                    if seg in self.letter_dict:
                        cur_pwd += random.choice(self.letter_dict[seg])
                    else:
                        print('Cannot find proper letters...')
                        ok = False
                        break
                elif seg_cat == 'D':
                    if seg in self.digit_dict:
                        cur_pwd += self.digit_dict[seg][0][0]
                    else:
                        print('Cannot find proper digits...')
                        ok = False
                        break
                elif seg_cat == 'S':
                    if seg in self.special_dict:
                        cur_pwd += self.digit_dict[seg][0][0]
                    else:
                        print('Cannot find proper special chars...')
                        ok = False
                        break
                elif seg_cat == 'B':
                    birth_ok = False
                    for birth_candidate in birth_candidates:
                        if len(birth_candidate) == seg_len:
                            cur_pwd += birth_candidate
                            birth_ok = True
                            break
                    if not birth_ok:
                        print('Cannot find proper birth candidates...')
                        ok = False
                        break
                elif seg_cat == 'N':
                    name_ok = False
                    for name_candidate in name_candidates:
                        if len(name_candidate) == seg_len:
                            cur_pwd += name_candidate
                            name_ok = True
                            break
                    if not name_ok:
                        print('Cannot find proper name candidates...')
                        ok = False
                        break
                elif seg_cat == 'E':
                    if len(email_prefix)<seg_len:
                        print('cannot find proper email candidates...')
                        ok = False
                        break
                    else:
                        cur_pwd += email_prefix[:seg_len]
                elif seg_cat == 'A':
                    if len(account)<seg_len:
                        print('cannot find proper account candidates...')
                        ok = False
                        break
                    else:
                        cur_pwd += account[:seg_len]
                elif seg_cat == 'C':
                    if len(cellphone)<seg_len:
                        print('cannot find proper cellphone candidates...')
                        ok = False
                        break
                    else:
                        cur_pwd += cellphone[:seg_len]
                elif seg_cat == 'I':
                    if len(id)<seg_len:
                        print('cannot find proper id candidates...')
                        ok = False
                        break
                    else:
                        cur_pwd += id[:seg_len]
                else:
                    print('Unknown tag...')
                    ok = False
                    break
            if ok:
                gen_tag += 1
                final_pwds.append(cur_pwd)
                if gen_tag == count:
                    break
        return final_pwds


if __name__ == '__main__':
    guesser = NaivePCFGGuesser(r'G:\workspace\python\qq\qqmining\web\data',
                               r'G:\workspace\python\qq\qqmining\web\data\dics\dic-0294.txt')
    guesses = guesser.gen_guesses(account='qingyuanxingsi',
                        birth='19920818',
                        name='梁肖',
                        email='qingyuanxingsi@163.com',
                        cellphone='15619026193',
                        id='429004199208185917')
    print(guesses)
    print(len(guesses))
