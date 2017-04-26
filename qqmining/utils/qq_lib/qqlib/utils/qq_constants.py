# -*- coding:utf-8 -*-


class QQConstants:
    """
    QQ constants
    """
    # friends constants
    # 异常
    friends_failure_tag = 0
    # 成功爬取
    friends_success_tag = 1
    # 账号退出，换账号重新登录
    friends_stop_tag = 2

    # manager constants
    manager_initial_tag = 0
    manager_profile_stage = 1
    manager_friend_stage = 2
    manager_snap_sleep = 60 * 0.015
    manager_kickout_sleep = 60 * 1
    # sleep 2 minutes if server busy
    manager_busy_sleep = 60 * 2
    manager_max_level = 7
    manager_task_queue_header = 'task_level_'
    manager_profile_queue = 'profile_queue'
    manager_proxy_sleep = 15
    manager_transfer_sleep = 60 * 2
    manager_snap = 0.5
    manager_json_dir = 'json_dir'
    manager_max_qq_cnt = 8

    # profile constants
    profile_other_tag = -2
    profile_stop_tag = 100

    # redis constants
    redis_separator = ':'

    # qq constants
    qq_normal_tag = 1
    qq_limit_tag = 0
    qq_no_tag = -1
    qq_other_tag = -2
    qq_violation_tag = -3
    qq_noopen_tag = -4
    qq_stop_tag = 100
    qq_denial_sleep = 60 * 0.2
    qq_friends_time_out = 8.0
    qq_max_cnt = 20000
    qq_long_sleep = 60 * 10

    # running stages
    qq_stage_running = 0
    qq_stage_captcha = 1
    qq_stage_recover = 2
    qq_stage_failure = 3
