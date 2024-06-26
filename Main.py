import os
import sys
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)),'src'))
from utils import *

if __name__ == '__main__':
    # * Main
    def execution(action):
        if action in dict_actions:
            [ func() for func in dict_actions[action] ] if isinstance(dict_actions[action], list) else dict_actions[action]()
        else:
            logging.getLogger("dev").error("Unknown action provided")

    execution(sys.argv[1] if len(sys.argv) > 1 else '-h')
