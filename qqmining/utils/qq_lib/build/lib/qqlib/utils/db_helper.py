# -*- encoding:utf-8 -*-

import datetime

import pymysql
import pymysql.cursors

"""
Database Access Helper Class
"""

if __name__ == '__main__':
    from qq_constants import QQConstants
else:
    from .qq_constants import QQConstants


class JsonProfile:
    """
    Json Profile Entity
    'id'
    'level'
    'type'
    'profile'
    'query_time'
    """

    def __init__(self, id, level, type, profile, query_time):
        self.id = id
        self.level = level
        self.type = type
        self.profile = profile
        self.query_time = query_time


class JsonInterests:
    """
    Json Interests Entities
    'uin': qq,
    'level': level,
    'interests': interests,
    'query_date': datetime.datetime.now()
    """

    def __init__(self, uin, level, interests, query_date):
        self.uin = uin
        self.level = level
        self.interests = interests
        self.query_date = query_date


class DBHelper:
    """
    Database helper class for data persistence
    """

    def __init__(self, host, db, username, password):
        self.host = host
        self.db = db
        self.username = username
        self.password = password

    # Cookies section
    def uin_exists(self, uin):
        """
        Determine if the qq number exists in cookie database
        :param qq:
        :return:
        """
        # Connect to the database
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                # Read a single record
                sql = "SELECT count(*) as cnt FROM `cookie_info` WHERE `cookie_qq`=%s"
                cursor.execute(sql, (uin,))
                result = cursor.fetchone()['cnt']
                return result != 0
        finally:
            connection.close()

    def get_all_cookies(self):
        """
        Get all user information
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = "select cookie_qq,cookie_pwd,cookie_dict,gtk from cookie_info where cookie_valid=1"
                cursor.execute(sql)
                final_result = []
                for cookie in cursor.fetchall():
                    final_result.append((cookie['cookie_qq'], cookie['cookie_pwd'], cookie['cookie_dict'], cookie['gtk']))
                return final_result
        except:
            return list()
        finally:
            connection.close()

    def insert_cookie(self, uin, cookie_dict, gtk, cookie_valid=True):
        """
        Insert cookies to the cookie pool
        :param uin:
        :param cookie_dict:
        :param gtk:
        :param cookie_valid:
        :return:
        """
        # Connect to the database
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        pwd = self.get_pwd_by_uin(uin)
        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = "INSERT INTO `cookie_info` (`cookie_qq`, `cookie_pwd`, `cookie_dict`, `gtk`, `cookie_valid`) " \
                      "VALUES (%s, %s, %s, %s, %s)"
                cursor.execute(sql, (uin, pwd, cookie_dict.encode('utf-8'), gtk, cookie_valid))

            # connection is not autocommit by default. So you must commit to save
            # your changes.
            connection.commit()
        finally:
            connection.close()

    def update_cookie(self, uin, cookie_dict, gtk, cookie_valid=True):
        """
        Update Cookie
        :param uin:
        :param cookie_dict:
        :param gtk:
        :param cookie_valid:
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        pwd = self.get_pwd_by_uin(uin)
        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = "update `cookie_info` set cookie_dict=%s, " \
                      "cookie_pwd=%s, gtk=%s, cookie_valid=%s where cookie_qq = %s"
                cursor.execute(sql, (cookie_dict.encode('utf-8'), pwd, gtk, cookie_valid, uin))
            connection.commit()
            return True
        except:
            return False
        finally:
            connection.close()

    # QQ Profile Section
    def qq_exists(self, qq):
        """
        Determine if the qq number exists in current database
        :param qq:
        :return:
        """
        # Connect to the database
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                # Read a single record
                sql = "SELECT count(*) as cnt FROM `qq_profile` WHERE `id`=%s"
                cursor.execute(sql, (qq,))
                result = cursor.fetchone()['cnt']
                return result != 0
        finally:
            connection.close()

    def insert_profile(self, qq, level, status, profile):
        """
        Insert the profile of the qq number into the database
        :param qq:
        :param level:
        :param status:
        :param profile:
        :return:
        """
        # Connect to the database
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = "INSERT INTO `qq_profile` (`id`, `level`,`type`,`profile`,`query_time`) " \
                      "VALUES (%s, %s, %s, %s, %s)"
                cursor.execute(sql, (qq, level, status, profile.encode('utf-8'), datetime.datetime.now()))

            # connection is not autocommit by default. So you must commit to save
            # your changes.
            connection.commit()
        finally:
            connection.close()

    def profile_cnt(self, query_day=None):
        """
        Get profile cnt
        :param query_day:
        :return:
        """
        # Connect to the database
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                # Read a single record
                if query_day is None:
                    sql = "SELECT count(*) as cnt FROM `qq_profile`"
                    cursor.execute(sql)
                else:
                    query_str = query_day + '%'
                    sql = "Select count(*) as cnt from `qq_profile` where query_time like %s"
                    cursor.execute(sql, (query_str,))
                result = cursor.fetchone()['cnt']
                return result
        finally:
            connection.close()

    def get_all_users(self):
        """
        Get all user information
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = "select * from qq_profile"
                cursor.execute(sql)
                final_result = []
                for user in cursor.fetchall():
                    final_result.append(
                        JsonProfile(user['id'], user['level'], user['type'], user['profile'],
                                    user['query_time']))
                return final_result
        except:
            return list()
        finally:
            connection.close()

    # machine stats section
    def machine_available(self):
        """
        Determine if any machine is available
        :return:
        """
        # Connect to the database
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                # Read a single record
                sql = "SELECT count(*) as cnt FROM `machine_stats` WHERE `qq_cnts`<%s"
                cursor.execute(sql, (QQConstants.manager_max_qq_cnt,))
                result = cursor.fetchone()['cnt']
                return result != 0
        finally:
            connection.close()

    def reset_machine_state(self):
        """
        Reset machine state
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                sql = "update machine_stats set qq_cnts=0"
                cursor.execute(sql)
            connection.commit()
        finally:
            connection.close()

    def get_machine_stats(self):
        """
        Get machine stats
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                sql = "select machine_id, ip, qq_cnts, query_date from machine_stats"
                cursor.execute(sql)
                final_result = []
                result = cursor.fetchall()
                if result is not None:
                    for r in result:
                        machine_id = r['machine_id']
                        ip = r['ip']
                        qq_cnts = r['qq_cnts']
                        query_date = r['query_date']
                        final_result.append((machine_id, ip, qq_cnts, query_date))
                return final_result
        except Exception as ex:
            print(ex)
            return None
        finally:
            connection.close()

    def remove_machine(self, machine_id):
        """
        remove machine
        :param machine_id:
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                sql = "delete from machine_stats where machine_id=%s"
                cursor.execute(sql, (machine_id,))
                connection.commit()
                return True
        except Exception as ex:
            print(ex)
            return False
        finally:
            connection.close()

    def add_device(self, ip):
        """
        Add new device
        :param ip:
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = "insert into machine_stats(ip, qq_cnts, query_date) " \
                      "values(%s, %s, %s)"
                cursor.execute(sql, (ip, 0, datetime.datetime.now()))
            connection.commit()
            return True
        except Exception as ex:
            print(ex)
            return False
        finally:
            connection.close()

    def device_ip_exists(self, ip):
        """
        Determine if the given device exists
        :param qq:
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                # Read a single record
                sql = "SELECT count(*) as cnt FROM `machine_stats` WHERE `ip`=%s"
                cursor.execute(sql, (ip,))
                result = cursor.fetchone()['cnt']
                return result != 0
        finally:
            connection.close()

    def get_idle_machine(self):
        """
        Determine if the given qq user exists
        :param qq:
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                # Read a single record
                sql = "SELECT machine_id, ip, qq_cnts FROM `machine_stats` order by qq_cnts asc limit 0,1"
                result = cursor.execute(sql)
                if result:
                    stat = cursor.fetchone()
                    id = stat['machine_id']
                    ip = stat['ip']
                    qq_cnts = stat['qq_cnts']
                    return (id, ip, qq_cnts)
                else:
                    return None
        finally:
            connection.close()

    def update_machine_overload(self, ip, overload):
        """
        Update machine overload
        :param ip:
        :param overload:
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = "update `machine_stats` set qq_cnts=%s, query_date=%s where ip = %s"
                cursor.execute(sql, (overload, datetime.datetime.now(), ip))
            connection.commit()
            return True
        except:
            return False
        finally:
            connection.close()

    # stats section
    def stats_exists(self, query_day, type):
        """
        Determine if the stats exists in db
        :param query_day:
        :param type:
        :return:
        """
        # Connect to the database
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                # Read a single record
                sql = "SELECT count(*) as cnt FROM `stats` WHERE `query_day`=%s and `type`=%s"
                cursor.execute(sql, (query_day, type))
                result = cursor.fetchone()['cnt']
                return result != 0
        finally:
            connection.close()

    def insert_daily_stats(self, type, query_day, cnt):
        """
        Insert daily stats
        :param type:
        :param query_day:
        :param cnt:
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = "INSERT INTO `stats` (`type`, `query_day`,`cnt`,`update_time`) " \
                      "VALUES (%s, %s, %s, %s)"
                cursor.execute(sql, (type, query_day, cnt, datetime.datetime.now()))

            # connection is not autocommit by default. So you must commit to save
            # your changes.
            connection.commit()
        finally:
            connection.close()

    def update_whole_stats(self, type, cnt):
        """
        Update whole stats
        :param type:
        :param cnt:
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = "update `stats` set cnt=%s, update_time=%s where type = %s and whole=%s"
                cursor.execute(sql, (cnt, datetime.datetime.now(), type, 1))
            connection.commit()
            return True
        except:
            return False
        finally:
            connection.close()

    def get_all_stats(self):
        """
        Get All Stats
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                sql = "select type, query_day, cnt, update_time,whole from stats"
                cursor.execute(sql)
                final_result = []
                result = cursor.fetchall()
                if result is not None:
                    for r in result:
                        type = r['type']
                        query_day = r['query_day']
                        cnt = r['cnt']
                        update_time = r['update_time']
                        whole = r['whole']
                        final_result.append((type, query_day, cnt, update_time, whole))
                return final_result
        except Exception as ex:
            print(ex)
            return None
        finally:
            connection.close()

    # qq_friends section
    def friends_cnt(self, query_day=None):
        """
        Get friends cnt
        :param query_day:
        :return:
        """
        # Connect to the database
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                # Read a single record
                if query_day is None:
                    sql = "SELECT count(*) as cnt FROM `qq_friends`"
                    cursor.execute(sql)
                else:
                    query_str = query_day + '%'
                    sql = "SELECT count(*) as cnt FROM `qq_friends` where query_time like %s"
                    cursor.execute(sql, (query_str,))
                result = cursor.fetchone()['cnt']
                return result
        finally:
            connection.close()

    def get_all_links(self):
        """
        Get all link information
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = "select * from qq_friends"
                cursor.execute(sql)
                final_result = []
                for link in cursor.fetchall():
                    final_result.append(
                        (link['qq'], link['friend'], link['level'], link['query_time'].strftime('%Y-%m-%d %H:%M:%S')))
                return final_result
        except:
            return list()
        finally:
            connection.close()

    def insert_friends(self, qq, level, friends):
        """
        Insert the friends of the qq number into the database
        :param qq:
        :param friends:
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = "INSERT INTO `qq_friends` (`qq`, `level`, `friend`,`query_time`) " \
                      "VALUES (%s, %s, %s, %s)"
                values = []
                cur_time = datetime.datetime.now()
                for friend in friends:
                    values.append((qq, level, friend, cur_time))
                cursor.executemany(sql, values)
            connection.commit()
        finally:
            connection.close()

    # qq interests section
    def interest_cnt(self, query_day=None):
        """
        Get interest count
        :param query_day:
        :return:
        """
        # Connect to the database
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                # Read a single record
                if query_day is None:
                    sql = "SELECT count(*) as cnt FROM `qq_interests`"
                    cursor.execute(sql)
                else:
                    query_str = query_day + '%'
                    sql = "SELECT count(*) as cnt FROM `qq_interests` where query_date like %s"
                    cursor.execute(sql, (query_str,))
                result = cursor.fetchone()['cnt']
                return result
        finally:
            connection.close()

    def insert_interests(self, qq, level, interests):
        """
        Insert interests of the given qq number
        :param qq:
        :param level:
        :param interests:
        :return:
        """
        # Connect to the database
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = "INSERT INTO `qq_interests` (`uin`, `level`, `interests`,`query_date`) " \
                      "VALUES (%s, %s, %s, %s)"
                cursor.execute(sql, (qq, level, interests.encode('utf-8'), datetime.datetime.now()))

            # connection is not autocommit by default. So you must commit to save
            # your changes.
            connection.commit()
        finally:
            connection.close()

    # profile tasks section
    def insert_profile_tasks(self, tasks):
        """
        Insert the profile tasks into the database
        :param tasks:
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = "INSERT INTO `profile_tasks` (`qq_num`, `level`,`state`,`insert_time`) " \
                      "VALUES (%s, %s, %s, %s)"
                values = []
                cur_time = datetime.datetime.now()
                for task in tasks:
                    values.append((task.qq_num, task.level, task.state, cur_time))
                cursor.executemany(sql, values)
            connection.commit()
        finally:
            connection.close()

    def fetch_profile_tasks(self, cnt):
        """
        Fetch profile tasks
        :param cnt:
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = "select record_id, qq_num, level from profile_tasks where state=%s limit 0,%s"
                cursor.execute(sql, (0, cnt))
                final_result = []
                record_list = list()
                qq_set = set()
                result = cursor.fetchall()
                if result is not None:
                    for r in result:
                        qq_num = r['qq_num']
                        if qq_num not in qq_set:
                            qq_set.add(qq_num)
                            record_id = r['record_id']
                            level = r['level']
                            record_list.append(record_id)
                            final_result.append((record_id, qq_num, level))
                return True, record_list, final_result
        except Exception as ex:
            print(ex)
            return False, list(), list()
        finally:
            connection.close()

    def update_records(self, records, tag, access_id=''):
        """
        Update the state of records to tag
        :param records:
        :param access_id:
        :param tag:
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = "update `profile_tasks` set state=%s, insert_time=%s, access_id=%s where record_id = %s"
                values = []
                cur_time = datetime.datetime.now()
                for record_id in records:
                    values.append((tag, cur_time, access_id, record_id))
                cursor.executemany(sql, values)
            connection.commit()
            return True
        except:
            return False
        finally:
            connection.close()

    def update_tasks(self, qq_list):
        """
        Update tasks
        :param qq_list:
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = "update `profile_tasks` set state=%s where qq_num = %s"
                values = []
                finish_tag = 2
                for uin in qq_list:
                    values.append((finish_tag, uin))
                cursor.executemany(sql, values)
            connection.commit()
            return True
        except:
            return False
        finally:
            connection.close()

    def restore_tasks(self, access_id):
        """
        Restore tasks
        :param access_id:
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = "update `profile_tasks` set state=0 where access_id = %s"
                cursor.execute(sql, (access_id,))
            connection.commit()
            return True
        except:
            return False
        finally:
            connection.close()

    # shuoshuo section
    def store_json(self, qq, index, json_text):
        """
        Store json into the database
        :param qq:
        :param index:
        :param json_text:
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = "INSERT INTO `shuoshuo` (`uin`, `index`,`json_text`,`query_time`) " \
                      "VALUES (%s, %s, %s, %s)"
                cursor.execute(sql, (qq, index, json_text, datetime.datetime.now()))

            # connection is not autocommit by default. So you must commit to save
            # your changes.
            connection.commit()
        finally:
            connection.close()

    # users section
    def get_available_users(self, cnt=5):
        """
        Get avaiable users
        :param cnt:
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = "select qq,pwd from users where state=0 and healthy=0 limit 0,%s"
                cursor.execute(sql, (cnt,))
                final_result = []
                for user in cursor.fetchall():
                    final_result.append(
                        (user['qq'], user['pwd']))
                return final_result
        except:
            return list()
        finally:
            connection.close()

    def get_pwd_by_uin(self, uin):
        """
        Get the password of the account by uin
        :param uin:
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                # Read a single record
                sql = "SELECT pwd FROM `users` WHERE `qq`=%s"
                cursor.execute(sql, (uin,))
                result = cursor.fetchone()
                if result is None:
                    return ''
                else:
                    return result['pwd']
        finally:
            connection.close()

    def reset_qq_state(self):
        """
        Reset qq state
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                sql = "update users set state=0"
                cursor.execute(sql)
            connection.commit()
        finally:
            connection.close()

    def fetch_users(self, cnt, mode=0):
        """
        Fetch users for mode
        :param cnt:
        :param mode:
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                # Create a new record
                recover_state = 2
                sql = "select qq, pwd, login_type, p_skey, p_uin from users " \
                      "where state=%s and healthy!=%s and mode=%s limit 0,%s"
                cursor.execute(sql, (0, recover_state, mode, cnt))
                final_result = []
                for r in cursor.fetchall():
                    qq = r['qq']
                    pwd = r['pwd']
                    login_type = r['login_type']
                    p_skey = r['p_skey']
                    p_uin = r['p_uin']
                    final_result.append((qq, pwd, login_type, p_skey, p_uin))
                return final_result
        except Exception as ex:
            print(ex)
            return None
        finally:
            connection.close()

    def update_batch_users(self, before_state, cur_state):
        """
        Update the state of users by batch
        :param before_state:
        :param cur_state:
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = "update `users` set healthy=%s where healthy = %s"
                cursor.execute(sql, (cur_state, before_state))
            connection.commit()
            return True
        except:
            return False
        finally:
            connection.close()

    def add_qq(self, qq, pwd='', login_type=0, p_skey='', p_uin='', mode=0, retry=0, task=0, remark='', worker_ip=''):
        """
        Add new qq number
        :param qq:
        :param pwd:
        :param login_type:
        :param p_skey:
        :param p_uin:
        :param mode:
        :param retry:
        :param task:
        :param remark:
        :param worker_ip:
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = "insert into users(qq,pwd,login_type,p_skey,p_uin,mode,retry,task,remark,worker_ip) " \
                      "values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(sql, (qq, pwd, login_type, p_skey, p_uin, mode, retry, task, remark, worker_ip))
            connection.commit()
            return True
        except Exception as ex:
            print(ex)
            return False
        finally:
            connection.close()

    def restore_users(self, access_id):
        """
        Restore users
        :param access_id:
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = "update `users` set used=0 where access_id = %s"
                cursor.execute(sql, (access_id,))
            connection.commit()
            return True
        except:
            return False
        finally:
            connection.close()

    def fetch_guest_user_state(self, remark=None):
        """
        fetch guest user state by remark
        :param remark:
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        cs_mode = 1
        try:
            with connection.cursor() as cursor:
                # Create a new record
                if remark is None:
                    sql = "select qq, pwd, state, recover_cnt, healthy, remark, mode from users where mode=%s"
                    cursor.execute(sql, (cs_mode,))
                else:
                    sql = "select qq, pwd, state, recover_cnt, healthy, remark, mode from users " \
                          "where mode=%s and remark=%s"
                    cursor.execute(sql, (cs_mode, remark))
                final_result = []
                for r in cursor.fetchall():
                    qq = r['qq']
                    pwd = r['pwd']
                    state = r['state']
                    recover_cnt = r['recover_cnt']
                    healthy = r['healthy']
                    remark = r['remark']
                    mode = r['mode']
                    final_result.append((qq, pwd, state, recover_cnt, healthy, remark, mode))
                return final_result
        except Exception as ex:
            print(ex)
            return None
        finally:
            connection.close()

    def update_qq_state(self, qq, login_type=0, p_skey='',
                        p_uin='', retry=0, healthy=0, task=0, recover=0, worker_ip=''):
        """
        Update qq state
        :param qq:
        :param login_type:
        :param p_skey:
        :param p_uin:
        :param retry:
        :param healthy:
        :param recover:
        :param username
        :param worker_ip:
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        recover_state = 2
        normal_state = 0
        if healthy == normal_state and recover == 0:
            state = 1
        else:
            state = 0
        try:
            with connection.cursor() as cursor:
                # Create a new record
                if healthy == recover_state:
                    sql = "update `users` set login_type=%s, p_skey=%s, p_uin=%s, state=%s, " \
                          "retry=%s, healthy=%s, task=%s, recover_cnt=recover_cnt+1, worker_ip=%s where qq = %s"
                else:
                    sql = "update `users` set login_type=%s, p_skey=%s, p_uin=%s, state=%s, " \
                          "retry=%s, healthy=%s, task=%s, worker_ip=%s where qq = %s"
                cursor.execute(sql, (login_type, p_skey, p_uin, state, retry, healthy, task, worker_ip, qq))
            connection.commit()
            return True
        except:
            return False
        finally:
            connection.close()

    def qq_user_exists(self, qq):
        """
        Determine if the given qq user exists
        :param qq:
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                # Read a single record
                sql = "SELECT count(*) as cnt FROM `users` WHERE `qq`=%s"
                cursor.execute(sql, (qq,))
                result = cursor.fetchone()['cnt']
                return result != 0
        finally:
            connection.close()

    def update_user_state(self, user_records, used, healthy=1, access_id=''):
        """
        Update User State
        :param user_records:
        :param access_id:
        :param used:
        :param healthy:
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = "update `users` set access_id=%s, used=%s, healthy=%s where user_id = %s"
                values = []
                for user_id in user_records:
                    values.append((access_id, used, healthy, user_id))
                cursor.executemany(sql, values)
            connection.commit()
            return True
        except:
            return False
        finally:
            connection.close()

    def fetch_user_state(self, mode=None):
        """
        Fetch user state
        :param mode:
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                # Create a new record
                if mode is None:
                    sql = "select qq, pwd, state, recover_cnt, healthy, remark, mode from users"
                    cursor.execute(sql)
                else:
                    sql = "select qq, pwd, state, recover_cnt, healthy, remark, mode from users " \
                          "where mode=%s"
                    cursor.execute(sql, (mode,))
                final_result = []
                for r in cursor.fetchall():
                    qq = r['qq']
                    pwd = r['pwd']
                    state = r['state']
                    recover_cnt = r['recover_cnt']
                    healthy = r['healthy']
                    remark = r['remark']
                    mode = r['mode']
                    final_result.append((qq, pwd, state, recover_cnt, healthy, remark, mode))
                return final_result
        except Exception as ex:
            print(ex)
            return None
        finally:
            connection.close()

    def fetch_retry_tasks(self, worker_ip, mode=None):
        """
        fetch retry tasks
        :param mode:
        :param worker_ip
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                # Create a new record
                if mode is None:
                    sql = "select qq, pwd, login_type, p_skey, p_uin, task from users " \
                          "where worker_ip=%s and retry=1"
                    cursor.execute(sql, (worker_ip,))
                else:
                    sql = "select qq, pwd, login_type, p_skey, p_uin, task from users " \
                          "where worker_ip=%s and retry=1 and mode=%s"
                    cursor.execute(sql, (worker_ip, mode,))
                final_result = []
                records = []
                result = cursor.fetchall()
                if result is not None:
                    for r in result:
                        qq = r['qq']
                        pwd = r['pwd']
                        login_type = r['login_type']
                        p_skey = r['p_skey']
                        p_uin = r['p_uin']
                        task = r['task']
                        records.append(qq)
                        final_result.append((qq, pwd, login_type, p_skey, p_uin, task))
                return records, final_result
        except Exception as ex:
            print(ex)
            return None, None
        finally:
            connection.close()

    def fetch_users2(self, user_cnt):
        """
        Fetch available users from the database
        :param user_cnt:
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = "select user_id, username, pwd from users where used=0 and healthy=1 limit 0,%s"
                cursor.execute(sql, (user_cnt,))
                final_result = []
                record_list = []
                for r in cursor.fetchall():
                    user_id = r['user_id']
                    username = r['username']
                    pwd = r['pwd']
                    record_list.append(user_id)
                    final_result.append((user_id, username, pwd))
                return True, record_list, final_result
        except Exception as ex:
            return False, list(), list()
        finally:
            connection.close()

    def fetch_available_users(self, cnt, mode=0):
        """
        Fetch users available for use
        :param cnt:
        :param mode:
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                # Create a new record
                normal_state = 0
                sql = "select qq, pwd from users " \
                      "where state=%s and healthy=%s and mode=%s limit 0,%s"
                cursor.execute(sql, (0, normal_state, mode, cnt))
                final_result = []
                for r in cursor.fetchall():
                    qq = r['qq']
                    pwd = r['pwd']
                    final_result.append((qq, pwd))
                return final_result
        except Exception as ex:
            print(ex)
            return None
        finally:
            connection.close()

    # sys users section
    def validate_user(self, username, pwd):
        """
        Validate user
        :param username:
        :param pwd:
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        guest_placeholder = -1
        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = "select pwd, guest from sys_users where `username`=%s"
                result = cursor.execute(sql, (username,))
                if result is None:
                    return False, guest_placeholder
                else:
                    record = cursor.fetchone()
                    cmp_pwd = record['pwd']
                    guest = record['guest']
                    if pwd == cmp_pwd:
                        return True, guest
                    else:
                        return False, guest_placeholder
        except:
            return False, guest_placeholder
        finally:
            connection.close()

    # user auth section
    def update_heart_beat(self, access_id, state=1):
        """
        Update heart beat
        :param access_id:
        :param state:
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = "update `user_auth` set state=%s, heartbeat=%s where access_id = %s"
                cursor.execute(sql, (state, datetime.datetime.now(), access_id))
            connection.commit()
            return True
        except:
            return False
        finally:
            connection.close()

    def get_alive_users(self):
        """
        get alive users
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = "select access_id,heartbeat from user_auth where state=1"
                cursor.execute(sql)
                final_result = []
                for user in cursor.fetchall():
                    final_result.append(
                        (user['access_id'], user['heartbeat']))
                return final_result
        except:
            return list()
        finally:
            connection.close()

    def get_access_key(self, access_id):
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                # Read a single record
                sql = "SELECT access_key FROM `user_auth` WHERE `access_id`=%s"
                cursor.execute(sql, (access_id,))
                result = cursor.fetchone()
                if result is None:
                    return None
                else:
                    return result['access_key']
        finally:
            connection.close()

    def user_working(self, access_id):
        """
        check if the user is working
        :param access_id:
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = "select state from `user_auth` where access_id = %s"
                cursor.execute(sql, (access_id,))
                result = cursor.fetchone()
                if result is None:
                    return False
                else:
                    state = result['state']
                    return state == 1
        except:
            return False
        finally:
            connection.close()

    # profile details section
    def insert_profile_batch(self, final_result):
        """
        Insert batch profiles
        :param final_result:
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = "INSERT INTO `qq_profile_details` (`uin`,`level`,`type`,`query_time`,`nickname`,`spacename`,`desc`,`signature`,`avatar`," \
                      "`sex_type`,`sex`,`animalsign_type`,`animalsign`,`constellation_type`,`constellation`,`age_type`,`age`," \
                      "`islunar`,`birthday_type`,`birthyear`,`birthday`,`bloodtype`,`address_type`,`country`,`province`," \
                      "`city`,`home_type`,`hco`,`hp`,`hc`,`marriage`,`lover`,`career`,`company`,`compaddr_type`," \
                      "`cco`,`cp`,`cc`,`cb`,`mailname`,`mailcellphone`,`mailaddr`,`like_number`,`profile_summary`) " \
                      "VALUES (%s, %s, %s, %s,%s,%s, %s, %s, %s,%s,%s," \
                      "%s, %s, %s, %s,%s,%s, %s, %s, %s,%s,%s," \
                      "%s, %s, %s, %s,%s,%s, %s, %s, %s,%s,%s," \
                      "%s, %s, %s, %s,%s,%s, %s, %s, %s,%s,%s)"
                values = []
                for profile in final_result:
                    values.append((profile.uin,
                                   profile.level,
                                   profile.type,
                                   profile.query_time,
                                   profile.nickname,
                                   profile.spacename,
                                   profile.desc,
                                   profile.signature,
                                   profile.avatar,
                                   profile.sex_type,
                                   profile.sex,
                                   profile.animalsign_type,
                                   profile.animalsign,
                                   profile.constellation_type,
                                   profile.constellation,
                                   profile.age_type,
                                   profile.age,
                                   profile.islunar,
                                   profile.birthday_type,
                                   profile.birthyear,
                                   profile.birthday,
                                   profile.bloodtype,
                                   profile.address_type,
                                   profile.country,
                                   profile.province,
                                   profile.city,
                                   profile.home_type,
                                   profile.hco,
                                   profile.hp,
                                   profile.hc,
                                   profile.marriage,
                                   profile.lover,
                                   profile.career,
                                   profile.company,
                                   profile.compaddr_type,
                                   profile.cco,
                                   profile.cp,
                                   profile.cc,
                                   profile.cb,
                                   profile.mailname,
                                   profile.mailcellphone,
                                   profile.mailaddr,
                                   profile.like_number,
                                   profile.profile_summary
                                   ))
                cursor.executemany(sql, values)
            connection.commit()
        except Exception as ex:
            print(ex)
        finally:
            connection.close()

    # Crawl records section
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
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = "insert into crawl_records(qq,start_time,end_time,proxy,cnt,task_type,reason) " \
                      "values(%s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(sql, (qq, start_time, end_time, use_proxy, cnt, task_type, reason))
            connection.commit()
            return True
        except Exception as ex:
            print(ex)
            return False
        finally:
            connection.close()

    def get_crawl_records(self):
        """
        Get crawl records
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                sql = "select qq, start_time, end_time, proxy, cnt, task_type, reason from crawl_records"
                cursor.execute(sql)
                final_result = []
                result = cursor.fetchall()
                if result is not None:
                    for r in result:
                        qq = r['qq']
                        start_time = r['start_time']
                        end_time = r['end_time']
                        proxy = r['proxy']
                        cnt = r['cnt']
                        task_type = r['task_type']
                        reason = r['reason']
                        final_result.append((qq, start_time, end_time, proxy,
                                             cnt, task_type, reason))
                return final_result
        except Exception as ex:
            print(ex)
            return None
        finally:
            connection.close()

    def get_records_by_qq(self, start_time=None, end_time=None):
        """
        Get records by qq number
        :param start_time:
        :param end_time:
        :return:
        """
        connection = pymysql.connect(host=self.host,
                                     user=self.username,
                                     password=self.password,
                                     db=self.db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                if start_time is None:
                    sql = "select qq, task_type, sum(cnt) as total_cnt from crawl_records group by qq, " \
                          "task_type order by qq asc, task_type asc"
                    cursor.execute(sql)
                else:
                    assert end_time is not None
                    sql = "select qq, task_type, sum(cnt) as total_cnt from crawl_records " \
                          "where start_time > %s and end_time < %s " \
                          "group by qq, task_type order by qq asc, task_type asc "
                    cursor.execute(sql, (start_time, end_time))
                final_result = []
                result = cursor.fetchall()
                if result is not None:
                    for r in result:
                        qq = r['qq']
                        task_type = r['task_type']
                        total_cnt = r['total_cnt']
                        final_result.append((qq, task_type, total_cnt))
                return final_result
        except Exception as ex:
            print(ex)
            return None
        finally:
            connection.close()


if __name__ == '__main__':
    db_helper = DBHelper('localhost', 'qq', 'root', 'batnos')
    print(db_helper.get_pwd_by_uin('1047929884'))
    print(db_helper.get_available_users(3))
