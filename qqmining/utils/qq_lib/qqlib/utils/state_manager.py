# -*- coding:utf-8 -*-

import threading

if __name__ == '__main__':
    from qq_state import QQState
    from qq_constants import QQConstants
else:
    from .qq_state import QQState
    from .qq_constants import QQConstants

state_lock = threading.RLock()


class StateManager:
    """
    State Manager
    """

    def __init__(self):
        self.state_dict = dict()

    def update_state(self, qq_num, state):
        with state_lock:
            if qq_num not in self.state_dict:
                new_state = QQState(qq_num, state)
                if state == QQConstants.qq_stage_recover:
                    new_state.recover_cnt = 1
                self.state_dict[qq_num] = new_state
            else:
                cur_state = self.state_dict[qq_num]
                cur_state.state = state
                if state == QQConstants.qq_stage_recover:
                    cur_state.recover_cnt += 1

    def get_state(self, qq_num):
        """
        Get qq state
        :param qq_num:
        :return:
        """
        with state_lock:
            if qq_num not in self.state_dict:
                return None
            else:
                return self.state_dict[qq_num]

    def split_dict(self):
        """
        Split dict into three dicts
        :return:
        """
        running_dict = dict()
        captcha_dict = dict()
        recover_dict = dict()
        failure_dict = dict()
        with state_lock:
            for key, value in self.state_dict.items():
                cur_state = value.state
                if cur_state == QQConstants.qq_stage_running:
                    running_dict[key] = value
                elif cur_state == QQConstants.qq_stage_captcha:
                    captcha_dict[key] = value
                elif cur_state == QQConstants.qq_stage_recover:
                    recover_dict[key] = value
                else:
                    failure_dict[key] = value
        return running_dict, captcha_dict, recover_dict, failure_dict
