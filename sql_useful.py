#+
# Common definitions for sql_utils programs.
#
# Copyright 2015 by Lawrence D'Oliveiro <ldo@geek-central.gen.nz>.
# Licensed under CC-BY-SA <http://creativecommons.org/licenses/by-sa/4.0/>.
#-

import mysql.connector
import apsw as sqlite
import getopt
import getpass

class AbstractDBMS :
    "abstract base class for DBMS-specific classes. Subclasses must override" \
    " all those with bodies that just raise NotImplementedError."

    conn_parm_names = None
      # subclass should replace this with a dictionary mapping constructor argument
      # names to functions that convert the values from strings to appropriate types.

    def __init__(self, **parms) :
        "subclass should set self.conn to actual DBMS-specific connection object."
        raise NotImplementedError
    #end __init__

    def close(self) :
        self.conn.close()
    #end close

    def string(self, s) :
        "returns a string literal which evaluates to s. Needed for those" \
        " times when the client API's automatic quoting isn't good enough."
        raise NotImplementedError
    #end string

    def iter(self, cmd, values = None, mapfn = lambda x : x) :
        "generator which executes cmd with values in a new cursor on conn," \
        " yielding the rows one at a time, optionally mapped through function mapfn."
        raise NotImplementedError
    #end iter

    blob_type = None
      # subclass should replace this with a string giving the name of the binary blob type.

    def cursor(self) :
        "returns a new DBMS-specific cursor object."
        return \
            self.conn.cursor()
    #end cursor

    def iter_table_names(self) :
        "yields a list of table names."
        return NotImplementedError
    #end iter_table_names

    def iter_columns(self, table_name) :
        "yields column definitions for the specified table." \
        " Each column definition is a dict containing at least type, not_null, default and" \
        " (if appropriate) primary_key_seq information."
        raise NotImplementedError
    #end iter_columns

    def iter_keys(self, table_name) :
        "yields information about non-primary keys for the specified table."
        return NotImplementedError
    #end iter_keys

#end AbstractDBMS

def parse_bool(s) :
    "interprets a user-specified string of various common forms as a boolean."
    s = s[0].lower()
    if s == "y" or s == "t" or s == "1" :
        result = True
    elif s == "n" or s == "f" or s == "0" :
        result = False
    else :
        raise ValueError("invalid bool value %s" % repr(s))
    #end if
    return \
        result
#end parse_bool

class MySQLDBMS(AbstractDBMS) :
    "handles connections to MySQL databases."

    conn_parm_names = {"database" : str, "host" : str, "password" : str, "port" : int, "user" : str}

    def __init__(self, **parms) :
        parms = dict(parms) # because I’m going to modify it
        for oldname, newname in \
            (
                ("database", "db"),
                ("password", "passwd"),
            ) \
        :
            if oldname in parms :
                parms[newname] = parms[oldname]
                del parms[oldname]
            #end if
        #end for
        parms["buffered"] = True # needed to avoid “unread result found” errors
        self.conn = mysql.connector.connect(**parms)
    #end __init__

    def string(self, s) :
        # or I could just call self.conn.converter.escape
        if s == None :
            result = "null"
        elif isinstance(s, bytes) :
            result = "X'" + "".join("%02X" % i for i in s) + "'"
        else :
            result = []
            for ch in str(s) :
                if ch == "\0" :
                    ch = "\\0"
                elif ch == "\010" :
                    ch = "\\b"
                elif ch == "\011" :
                    ch = "\\t"
                elif ch == "\012" :
                    ch = "\\n"
                elif ch == "\015" :
                    ch = "\\r"
                elif ch == "\032" :
                    ch = "\\z"
                elif ch == "'" or ch == "\"" or ch == "\\" :
                    ch = "\\" + ch
                #end if
                result.append(ch)
            #end for
            result = "\"" + "".join(result) + "\""
        #end if
        return \
            result
    #end string

    def iter(self, cmd, values = None, mapfn = lambda x : x) :
        cursor = self.conn.cursor()
        cursor.execute(cmd, values)
        while True :
            yield mapfn(next(cursor))
        #end while
    #end iter

    blob_type = "binary"

    def iter_table_names(self) :
        "note this will only work if there is a default database."
        return \
            self.iter(cmd = "show tables", mapfn = lambda x : x[0])
    #end iter_table_names

    def iter_columns(self, table_name) :
        primary_key_seq = {}
        for item in self.iter(cmd = "show keys from %s where key_name = 'PRIMARY'" % table_name) :
            name = item[4]
            primary_key_seq[name] = item[3]
        #end for
        for item in self.iter(cmd = "show columns from %s" % table_name) :
            name = item[0]
            col = {"name" : name, "type" : item[1], "not_null" : item[2] != "NO", "default" : item[4]}
            if name in primary_key_seq :
                col["primary_key_seq"] = primary_key_seq[name]
            #end if
            yield col
        #end for
    #end iter_columns

    def iter_keys(self, table_name) :
        keys = self.iter(cmd = "show keys from %s where key_name != 'PRIMARY'" % table_name)
        last_key_name = None
        while True :
            item = next(keys, None)
            if item != None :
                key_name = item[2]
                unique = item[1] == 0
                field_name = item[4]
                key_seq = item[3]
            #end if
            if item == None or key_name != last_key_name :
                if last_key_name != None :
                    yield entry
                #end if
                if item == None :
                    break
                entry = {"name" : key_name, "unique" : unique, "fields" : []}
                last_key_name = key_name
                last_key_seq = 0
            #end if
            assert key_seq == last_key_seq + 1, "key fields not being returned in sequence"
            entry["fields"].append(field_name)
            last_key_seq = key_seq
        #end while
    #end iter_keys

#end MySQLDBMS

class SQLiteDBMS(AbstractDBMS) :
    "handles connections to SQLite databases."

    conn_parm_names = {"create" : parse_bool, "filename" : str, "write" : parse_bool}

    def __init__(self, filename, create = False, write = True) :
        self.conn = sqlite.Connection \
          (
            filename,
            flags =
                    (sqlite.SQLITE_OPEN_READONLY, sqlite.SQLITE_OPEN_READWRITE)[write]
                |
                    (0, sqlite.SQLITE_OPEN_CREATE)[create]
          )
    #end __init__

    def string(self, s) :
        return \
            sqlite.format_sql_value(s)
    #end string

    def iter(conn, cmd, values = None, mapfn = lambda x : x) :
        cu = conn.cursor()
        result = cu.execute(cmd, values)
        while True :
            yield mapfn(next(result))
        #end while
    #end iter

    blob_type = "blob"

    def iter_table_names(self) :
        return \
            self.iter \
              (
                cmd = "select name from sqlite_master where type = 'table'",
                mapfn = lambda x : x[0]
              )
    #end iter_table_names

    def iter_columns(self, table_name) :
        for item in self.iter(cmd = "pragma table_info(%s)" % table_name) :
            name = item[1]
            col = {"name" : name, "type" : item[2], "not_null" : item[3] != 0, "default" : item[4]}
            if item[5] != 0 :
                col["primary_key_seq"] = item[5]
            #end if
            yield col
        #end for
    #end iter_columns

    def iter_keys(self, table_name) :
        for item in \
            self.iter \
              (
                cmd = "select sql from sqlite_master where type = 'index'",
                mapfn = lambda x : x[0]
              ) \
        :
            match = re.search(r"^create( unique)? index (\w+) on (\w+)\s*\(([^\)]+)\)", item, re.IGNORECASE)
              # ignore any trailing “where” clause”
            unique = match.group(1) != None
            key_name = match.group(2)
            table_name = match.group(3)
            fields = list(field.strip() for field in match.group(4).split(","))
            yield {"name" : key_name, "unique" : unique, "fields" : fields}
        #end for
    #end iter_keys

#end SQLiteDBMS

DBMSClasses = \
    {
        "mysql" : MySQLDBMS,
        "sqlite" : SQLiteDBMS,
    }

class BulkInserter :
    """bulk insertion of lots of records into an SQL table."""

    def __init__(self, conn, table_name, field_names, ignore_duplicates) :
        """conn is the MySQL connection to use; table_name is the name of the
        table into which to insert records; field_names is the list of field names;
        and ignore_duplicates is True to ignore duplicate insertions, False to
        report an error."""
        self.sql = conn
        self.cursor = None
        self.table_name = table_name
        self.field_names = tuple(field_names)
        self.ignore_duplicates = ignore_duplicates
        self.insert_limit = 500 # could even be larger for MySQL, but SQLite can’t handle more than this
        self.field_values = []
    #end __init__

    def add_record(self, field_values) :
        """adds another record to the table. field_values is the list of field
        values, corresponding in order to the previously-specified field_names."""
        if len(self.field_values) == self.insert_limit :
            self.done_insert()
        #end if
        if self.cursor == None :
            self.cursor = self.sql.cursor()
        #end if
        this_record = []
        if type(field_values) == dict :
            for field_name in self.field_names :
                this_record.append(self.sql.string(field_values[field_name]))
            #end for
        else :
            for field_value in field_values :
                this_record.append(self.sql.string(field_value))
            #end for
        #end if
        self.field_values.append(this_record)
    #end add_record

    def done_insert(self) :
        """Call this after the last add_record call to make sure all insertions
        have been flushed to the table."""
        if len(self.field_values) != 0 :
            insert = "insert" # used to also have “delayed” but this is MySQL-specific
            if self.ignore_duplicates :
                insert += " ignore"
            #end if
            insert += " into " + self.table_name + " (" + ", ".join(self.field_names) + ") values"
            first_insert = True
            for this_record in self.field_values :
                if first_insert :
                    first_insert = False
                else :
                    insert += ","
                #end if
                insert += " (" + ", ".join(this_record) + ")"
            #end for
            self.cursor.execute(insert)
            self.cursor.close()
            self.cursor = None
            self.field_values = []
        #end if
    #end done_insert

#end BulkInserter

def parse_dbms_params(paramsstr, doing_what) :
    "parses paramsstr as a colon-separated string; the item before the first colon" \
    " is expected to be the DBMS name, while subsequent items take the form param=value," \
    " where the valid parameter names and types come from the valid arguments to the" \
    " DBMS class constructor."
    items = paramsstr.split(":")
    if len(items) == 0 :
        raise getopt.GetoptError \
          (
            "need to specify DBMS %s" % (doing_what,)
          )
    #end if
    dbmstype = items[0]
    if dbmstype not in DBMSClasses :
        raise getopt.GetoptError \
          (
            "unrecognized DBMS “%s” %s" % (dbmstype, doing_what)
          )
    #end if
    dbms_class = DBMSClasses[dbmstype]
    conn_params = {}
    unrecognized = set()
    for item in items[1:] :
        keyword, value = item.split("=", 1)
        if keyword in dbms_class.conn_parm_names and keyword != "create" :
            # no point supporting “create” in any of these tools, since
            # destination table must already exist
            conn_params[keyword] = dbms_class.conn_parm_names[keyword](value)
        else :
            unrecognized.add(keyword)
        #end if
    #end for
    if len(unrecognized) != 0 :
        raise getopt.GetoptError \
          (
                "unrecognized %s connection params “%s” %s"
            %
                (dbmstype, ",".join(sorted(unrecognized)), doing_what)
          )
    #end if
    if conn_params.get("password") == "" :
        conn_params["password"] = getpass.getpass \
          (
            prompt =
                    "%s password%s: "
                %
                    (
                        dbmstype,
                        ("", " for %s" % conn_params.get("database", ""))["database" in conn_params]
                    )
          )
    #end if
    return \
        dbms_class(**conn_params)
#end parse_dbms_params
