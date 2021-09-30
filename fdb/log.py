#coding:utf-8
#
#   PROGRAM/MODULE: fdb
#   FILE:           log.py
#   DESCRIPTION:    Python driver for Firebird - Firebird server log parser
#   CREATED:        11.4.2018
#
#  Software distributed under the License is distributed AS IS,
#  WITHOUT WARRANTY OF ANY KIND, either express or implied.
#  See the License for the specific language governing rights
#  and limitations under the License.
#
#  The Original Code was created by Pavel Cisar
#
#  Copyright (c) Pavel Cisar <pcisar@users.sourceforge.net>
#  and all contributors signed below.
#
#  All Rights Reserved.
#  Contributor(s): ______________________________________.
#
# See LICENSE.TXT for details.

from fdb import ParseError
from datetime import datetime
from collections import namedtuple
from locale import LC_ALL, LC_CTYPE, getlocale, setlocale, resetlocale
import sys

LogEntry = namedtuple('LogEntry', 'source_id,timestamp,message')

_LOCALE_ = LC_CTYPE if sys.version_info[0] == 3 else LC_ALL

def parse(lines):
    """Parse Firebird server log and yield named tuples describing individual log entries/events.

    Args:
        lines: Iterable of lines from Firebird server log.

    Raises:
        fdb.ParseError: When any problem is found in input stream.
"""
    line_no = 0
    locale = getlocale() # (_LOCALE_)
    if sys.platform == 'win32':
        setlocale(_LOCALE_, 'English_United States')
    else:
        setlocale(_LOCALE_, 'en_US')
    try:
        clean = (line.strip() for line in lines)
        entry_lines = []
        timestamp = None
        source_id = 'UNKNOWN'
        for line in clean:
            line_no += 1
            if line == '':
                continue
            items = line.split()
            if len(items) > 5:  # It's potentially new entry
                try:
                    new_timestamp = datetime.strptime(' '.join(items[len(items)-5:]),
                                                      '%a %b %d %H:%M:%S %Y')
                except ValueError:
                    new_timestamp = None
                if new_timestamp is not None:
                    if entry_lines:
                        yield LogEntry(source_id=source_id, timestamp=timestamp,
                                       message='\n'.join(entry_lines))
                        entry_lines = []
                    # Init new entry
                    timestamp = new_timestamp
                    source_id = ' '.join(items[:len(items)-5])
                else:
                    entry_lines.append(line)
            else:
                entry_lines.append(line)
        if entry_lines:
            yield LogEntry(source_id=source_id, timestamp=timestamp, message='\n'.join(entry_lines))
    except Exception as e:
        raise ParseError("Can't parse line %d\n%s" % (line_no, e.message))
    finally:
        if locale[0] is None:
            if sys.platform == 'win32':
                setlocale(_LOCALE_, '')
            else:
                resetlocale(_LOCALE_)
        else:
            setlocale(_LOCALE_, locale)
