import logging
import sys

logger = logging.getLogger()
hdlr = logging.StreamHandler(sys.stdout)
hdlr.setFormatter(logging.Formatter('$module:$lineno $msg', style='$'))
logger.addHandler(hdlr)
logger.setLevel(logging.DEBUG)
