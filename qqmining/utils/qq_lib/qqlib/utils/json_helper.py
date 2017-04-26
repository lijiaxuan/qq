# -*- coding:utf-8 -*-

import datetime
import os
import pickle
import shutil


class JsonHelper:
    """
    Json Helper Class to storage json
    """

    def __init__(self, cur_dir, json_dir):
        self.cur_dir = cur_dir
        self.json_dir = json_dir
        self.storage_dir = os.path.join(cur_dir, json_dir)
        if os.path.exists(self.storage_dir):
            shutil.rmtree(self.storage_dir, True)
        os.mkdir(self.storage_dir)

    def qq_exists(self, qq):
        """
        Determine if the qq number exists in current database
        :param qq:
        :return:
        """
        qq_json_path = os.path.join(self.storage_dir, qq + '_profile.pkl')
        return os.path.exists(qq_json_path)

    def insert_profile(self, qq, level, status, profile):
        """
        Insert the profile of the qq number into the database
        :param qq:
        :param level:
        :param status:
        :param profile:
        :return:
        """
        json_text = {
            'id': qq,
            'level': level,
            'type': status,
            'profile': profile,
            'query_time': datetime.datetime.now()
        }
        qq_file = open(os.path.join(self.storage_dir, qq + '_profile.pkl'), 'wb')
        pickle.dump(json_text, qq_file)
        qq_file.close()

    def fetch_users(self, cnt, mode=0):
        """
        Fetch users for mode
        :param cnt:
        :param mode:
        :return:
        """
        pass

    def insert_crawl_record(self, qq, start_time, end_time,
                            use_proxy, cnt, task_type, reason):
        """
        Insert crawl records
        :param qq:
        :param start_time:
        :param end_time:
        :param use_proxy:
        :param cnt:
        :param task_type:
        :param reason:
        :return:
        """
        pass

    def update_qq_state(self, qq, login_type=0, p_skey='',
                        p_uin='', retry=0, healthy=0):
        """
        Update qq state
        :param qq:
        :param login_type:
        :param p_skey:
        :param p_uin:
        :param retry:
        :param healthy:
        :return:
        """
        pass

    def insert_interests(self, qq, level, interests):
        """
        Insert interests of the given qq number
        :param qq:
        :param level:
        :param interests:
        :return:
        """
        json_interests = {
            'uin': qq,
            'level': level,
            'interests': interests,
            'query_date': datetime.datetime.now()
        }
        qq_file = open(os.path.join(self.storage_dir, qq + '_interests.pkl'), 'wb')
        pickle.dump(json_interests, qq_file)
        qq_file.close()

    def insert_friends(self, qq, level, friends):
        """
        Insert the friends of the qq number into the database
        :param qq:
        :param level:
        :param friends:
        :return:
        """
        # TODO:Not implemented
        pass

    def insert_profile_tasks(self, tasks):
        """
        Insert the profile tasks into the database
        :param tasks:
        :return:
        """
        # TODO:Not implemented
        pass

    def store_json(self, qq, index, json_text):
        """
        Store json into the database
        :param qq:
        :param index:
        :param json_text:
        :return:
        """
        # TODO:Not implemented
        pass

    def fetch_profile_tasks(self, cnt):
        """
        Fetch profile tasks
        :param cnt:
        :return:
        """
        # TODO:Not implemented
        pass

    def update_records(self, records, tag):
        """
        Update the state of records to tag
        :param records:
        :param tag:
        :return:
        """
        # TODO:Not implemented
        pass
