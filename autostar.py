import argparse
import json
import sqlite3
import sys
from collections import defaultdict

import mozlog
import requests
import thclient

logger = None

actions = {name:i for i, name in enumerate(["test_result", "log", "crash"])}
actions_by_number = {v:k for k,v in actions.iteritems()}
log_levels = mozlog.structuredlog.log_levels
levels_by_number = {v:k for k,v in log_levels.iteritems()}

class ErrorLine(object):
    def __init__(self, id, action, line, test=None, subtest=None, status=None, expected=None,
                 message=None, stack=None, signature=None, stackwalk_stdout=None,
                 stackwalk_stderr=None, level=None, autostar_id=None, job_id=None):
        self.id = id
        self.action = actions_by_number[action]
        self.line = line
        self.test = test
        self.subtest = subtest
        self.status = status
        self.expected = expected
        self.message = message
        self.stack = stack
        self.signature = signature
        self.stackwalk_stdout = stackwalk_stdout
        self.stackwalk_stderr = stackwalk_stderr
        self.level = levels_by_number[level] if level is not None else None
        self.autostar_id = autostar_id
        self.job_id = job_id

    def __str__(self):
        attrs = self.__dict__
        return "ErrorLine: %s (%s)" % (self.action, repr({k:v for k,v in attrs.iteritems()
                                                          if v is not None}))

    def as_dict(self):
        pass

    def set(self, conn, attr, value):
        if attr == "action":
            db_value = actions[value]
        elif attr == "level":
            db_value = log_levels[level]
        else:
            db_value = value

        assert hasattr(self, attr)
        c = conn.cursor()
        c.execute("""UPDATE errors SET %s = ? WHERE id = ?""" % attr, (db_value, self.id))
        setattr(self, attr, value)
        conn.commit()

class Job(object):
    def __init__(self, id, job_type, ref_data_name, result, errors_processed, result_set_id):
        self.id = id
        self.job_type = job_type
        self.ref_data_name = ref_data_name
        self.result = result
        self.errors_processed = errors_processed
        self.result_set_id = result_set_id
        self.error_lines = []

    def set(self, conn, attr, value):
        assert hasattr(self, attr)
        c = conn.cursor()
        c.execute("""UPDATE errors SET %s = ? WHERE id = ?""" % attr, (value, self.id))
        setattr(self, attr, value)
        conn.commit()


def setup_database(conn, repo, revision):
    c = conn.cursor()

    c.execute("""CREATE TABLE results_sets
    (id integer primary key, sha1 text)""")

    c.execute("""CREATE TABLE jobs
    (id integer primary key, job_type text, ref_data_name text, result text, errors_processed int, result_set_id int)""")

    c.execute("""CREATE TABLE errors
    (id integer primary key autoincrement, action integer, line integer, test text, subtest text, status text, expected text, message text, stack text, signature text, stackwalk_stdout text, stackwalk_stderr text, level integer, autostar_id integer, job_id integer)""")
    conn.commit()

    c.execute("""CREATE TABLE autostar
    (id integer primary key autoincrement)""")

    c.execute("""CREATE TABLE meta
    (last_revision_id text, repository text)""")

    c.execute("""INSERT INTO meta (repository, last_revision_id)
    VALUES (?,?)""", (repo, revision))
    conn.commit()

def get_meta(conn):
    c = conn.cursor()
    c.execute("""SELECT repository, last_revision_id FROM meta LIMIT 1""")
    row = c.fetchone()

    if not row:
        raise ValueError("Need to set up db with repo and revision")
    else:
        repo, last_rev = row

    return repo, last_rev


def get_result_sets(client, repository, revisions, load_from):
    if load_from:
        assert len(revisions) == 1
        return client.get_resultsets(repository, count=None, fromchange=revisions[0])
    else:
        rv = []
        for revision in revisions:
            rv += client.get_resultsets(repository, count=1, revision=revision)
        return rv

def get_jobs(client, repository, result_set):
    return client.get_jobs(repository, result_set_id=result_set['id'], count=None)

def get_finished_jobs(client, repository, result_set):
    return [item for item in get_jobs(client, repository, result_set)
            if item["state"] == "completed"]

def get_new_jobs(conn, jobs):
    c = conn.cursor()
    job_ids = [item["id"] for item in jobs]
    query = """SELECT id FROM jobs WHERE id IN (%s)""" % ("?," * (len(jobs) - 1) + "?")
    c.execute(query, tuple(item["id"] for item in jobs))
    existing_jobs = set(item[0] for item in c.fetchall())
    return [item for item in jobs if item["id"] not in existing_jobs]


def insert_errors(conn, job_id, error_lines):
    c = conn.cursor()
    for line in error_lines:
        c.execute("""INSERT INTO errors (action, line, test, subtest, status, expected, message, stack, signature, stackwalk_stdout, stackwalk_stderr, level, job_id)
                     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                  (actions[line["action"]], line["line"], line.get("test"),
                   line.get("subtest"), line.get("status"), line.get("expected"),
                   line.get("message"), line.get("stack"), line.get("signature"),
                   line.get("stackwalk_stdout"), line.get("stackwalk_stderr"),
                   log_levels.get(line.get("level")), job_id))
    conn.commit()


def insert_jobs(conn, result_set_id, jobs, error_lines_by_job):
    rv= []
    c = conn.cursor()
    for job in jobs:
        c.execute("""INSERT OR IGNORE INTO jobs (id, job_type, ref_data_name, result,
                                                 errors_processed, result_set_id)
                    VALUES (?, ?, ?, ?, 0, ?)""",
                  (job["id"], job["job_type_name"], job["ref_data_name"], job["result"],
                   result_set_id))
        if job["id"] in error_lines_by_job:
            insert_errors(conn, job["id"], error_lines_by_job[job["id"]])

    conn.commit()
    return rv


def job_has_errors(job):
    return job["result"] in ["testfailed", "busted"]


def get_artifacts(client, repository, job):
    return client.get_artifacts(project=repository, job_id=job["id"],
                                name="Job Info")

def get_error_urls(client, repository, jobs):
    rv = {}
    for job in jobs:
        if job_has_errors(job):
            artifacts = get_artifacts(client, repository, job)
            error_url = get_error_url(artifacts)
            if error_url:
                rv[job["id"]] = error_url
    return rv


def fetch_summary(url):
    resp = requests.get(url)
    resp.raise_for_status()
    return [json.loads(line) for line in resp.iter_lines()]


def get_error_lines(client, error_urls):
    return {key:fetch_summary(url) for key,url in error_urls.iteritems()}


def get_error_url(artifacts):
    for item in artifacts:
        data = item.get("blob", {}).get("job_details", [])
        for item in data:
            logger.debug("Processing artifact %s %s" % (item.get("title", None), item.get("value")))
            if (item.get("content_type") == "link" and
                item.get("title") == "artifact uploaded" and
                item.get("value").endswith("_errorsummary.log")):

                logger.debug("Got error url %s" % item["url"])
                return item["url"]


def dict_from_query(description, data):
    keys = (item[0] for item in description)
    return {key:value for key, value in zip(keys, data)}


def load_jobs(conn, result_set_id):
    c = conn.cursor()
    c.execute("""SELECT * from jobs WHERE result_set_id = ?""", (result_set_id,))
    cols = c.description
    rv = []
    for row in c.fetchall():
        job = Job(**dict_from_query(cols, row))
        c1 = conn.cursor()
        c1.execute("""SELECT * FROM errors WHERE job_id = ?""", (job.id,))
        error_cols = c1.description
        error_lines = c1.fetchall()
        if error_lines:
            lines = []
            for error_line in error_lines:
                lines.append(ErrorLine(**dict_from_query(error_cols, error_line)))
            job.error_lines = lines
        rv.append(job)

    return rv

def by_ref_type(jobs):
    rv = defaultdict(list)
    for item in jobs:
        rv[item.ref_data_name].append(item)
    return rv


def precise_test_matcher(conn, error_lines):
    c = conn.cursor()
    for error_line in error_lines:
        logger.debug("Looking for test match on line %s" % error_line[0])
        line = error_line[0]

        if line.action == "test_result":
            query = """SELECT autostar_id FROM errors WHERE
            action = ? AND test = ? AND status = ? AND
            expected = ? AND autostar_id IS NOT NULL"""
            params = [actions[line.action], line.test,
                      line.status, line.expected]

            for maybe_null in ["subtest", "message", "stack"]:
                value = getattr(line, maybe_null)
                if value is None:
                    query += " AND %s IS NULL" % maybe_null
                else:
                    query += " AND %s = ?" % maybe_null
                    params.append(value)

            query += " GROUP BY autostar_id"
            c.execute(query, tuple(params))

            rows = c.fetchall()

            logger.debug("Found %i matching rows" % len(rows))

            assert len(rows) <= 1, "Need to fix the case where we have more than one exact match for a test failure"
            if rows:
                logger.info("PreciseTestMatcher matched line %s" % error_line[0])
                error_line[1].append((rows[0][0], 1))


def precise_log_matcher(conn, error_lines):
    c = conn.cursor()
    for error_line in error_lines:
        line = error_line[0]
        if line.action == "log":
            c.execute("""SELECT autostar_id FROM errors WHERE
            action = ? AND level = ? AND message = ?
            GROUP BY autostar_id
            """,
                      (actions["log"], line.level, line.message))
            rows = c.fetchall()

            assert len(rows) <= 1, "Need to fix the case where we have more than one exact match for a log message"
            if rows:
                logger.info("PreciseLogMatcher matched line %s" % error_line)
                error_line[1].append((rows[0][0], 1))

def get_creators():
    return [test_failure_creator]

def test_failure_creator(error_lines):
    rv = []
    for i, error_line in enumerate(error_lines):
        if (error_line.test and error_line.status and error_line.expected):
            rv.append(i)
    return rv


def get_matchers():
    return [precise_test_matcher, precise_log_matcher]


def match_errors(conn, matchers, job):
    error_lines = [(line, []) for line in job.error_lines if not line.autostar_id]
    for matcher in matchers:
        matcher(conn, error_lines)

    for line, matches in error_lines:
        best_match_id = get_best_match(matches)
        if best_match_id:
            logger.info("Got a match for error line %s with autostar id %i" %
                        (line, best_match_id))
            line.set(conn, "autostar_id", best_match_id)


def get_best_match(matches):
    if not matches:
        return

    ordered_matches = sorted(matches[:], key=lambda x:x[1])
    return ordered_matches[-1][0]


new_matched_tests = set()

def get_by_type(jobs):
    rv = defaultdict(list)
    for item in jobs:
        rv[item.job_type].append(item)
    return rv


def add_new_intermittents(conn, all_jobs):
    all_failed_jobs = [item for item in all_jobs if item.error_lines]

    jobs_by_type = get_by_type(all_jobs)

    logger.info("Looking for intermittents in %i jobs" % len(all_failed_jobs))
    for type, jobs in jobs_by_type.iteritems():
        # The approach here is currently to look for new intermittents to add, one at a time
        # and then rerun the matching on other jobs
        # TODO: limit the possible matches to those that have just been added
        logger.debug("%s: %i jobs" % (type, len(jobs)))
        if len(jobs) <= 1:
            logger.debug("Too few jobs in the current set")
            continue

        # For now conservatively assume that we can only mark new intermittents if
        # one run in the current set fully passes
        if not any(job.result == "success" for job in jobs):
            logger.debug("No successful jobs to compare against")
            continue

        for i, job in enumerate(jobs):
            logger.debug("Processing job %i" % i)
            if not job.error_lines:
                continue

            found_new_matches = False

            for creator in get_creators():
                unmached_lines = [item for item in job.error_lines if item.autostar_id is None]
                if unmached_lines:
                    logger.debug("Found %i unmached lines" % len(unmached_lines))
                line_indicies = creator(unmached_lines)
                for index in line_indicies:
                    error_line = job.error_lines[index]
                    found_new_matches = True
                    new_matched_tests.add(error_line.test)
                    logger.info("Found new intermittent %s" % error_line)
                    c = conn.cursor()
                    c.execute("""INSERT INTO autostar DEFAULT VALUES""")
                    conn.commit()
                    error_line.set(conn, "autostar_id", c.lastrowid)
            if found_new_matches:
                for rematch_job in all_failed_jobs:
                    logger.debug(rematch_job)
                    if rematch_job == job:
                        continue
                    logger.debug("Trying rematch on job %i (%s)" % (rematch_job.id, rematch_job.ref_data_name))
                    match_errors(conn, get_matchers(), rematch_job)


def job_errors_processed(conn, job):
    c = conn.cursor()
    c.execute("""SELECT errors_processed FROM jobs WHERE id = ?""",
              (job.id,))
    return bool(c.fetchone()[0])


def load_error_lines(conn, job):
    c = conn.cursor()
    c.execute("""SELECT id, action, line, test, subtest, status, expected, message, stack, signature, stackwalk_stdout, stackwalk_stderr, level, autostar_id FROM errors WHERE job_id = ?""",
              (job.id,))
    rows = c.fetchall()
    cols = [item[0] for item in c.description]

    rv = []
    for row in rows:
        row_data = {cols[i]:row[i] for i in xrange(len(cols))}

        row_id = row_data.pop("id")
        autostar_id = row_data.pop("autostar_id")

        action = actions_by_number[row_data["action"]]

        if action == "test_result":
            data = {"test": row_data.get("test"),
                    "subtest": row_data.get("subtest"),
                    "status": row_data.get("status"),
                    "expected": row_data.get("expected"),
                    "message": row_data.get("message"),
                    "stack": row_data.get("stack")}
        elif action == "crash":
            data = {"test": row_data.get("test"),
                    "signature": row_data.get("signature"),
                    "stackwalk_stdout": row_data.get("stackwalk_stdout"),
                    "stackwalk_stderr": row_data.get("stackwalk_stderr")}
        elif action == "log":
            data = {"level": levels_by_number[row_data.get("level")],
                    "message": row_data.get("message")}
        else:
            logger.critical("Unrecognised action %s" % action)
            sys.exit(1)

        data["action"] = action
        data["line"] = row_data["line"]

        error_line = ErrorLine(data)
        error_line.row_id = row_id
        error_line.autostar_id = autostar_id
        rv.append(error_line)
    return rv


def process_errors(conn, jobs):
    jobs_by_ref_type = by_ref_type(jobs)
    for job_type, ref_type_jobs in jobs_by_ref_type.iteritems():
        for job in ref_type_jobs:
            if not job.errors_processed and job.error_lines:
                logger.info("matching errors job %s (%s)" % (job.id, job_type))
                match_errors(conn, get_matchers(), job)


def update_meta(conn, new_last_rev):
    c = conn.cursor()
    c.execute("""UPDATE meta SET last_revision_id = ?""", (new_last_rev,))
    conn.commit()


def parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--setup", nargs=2, help="Recreate the database from scratch. Takes two arguments, a")
    parser.add_argument("--db-name", action="store", default="star.db", help="Name of the database file to use.")
    parser.add_argument("--repository", action="store", help="Repository to run against")
    parser.add_argument("--rev", action="append", help="Revision id to use")
    mozlog.commandline.add_logging_group(parser)
    return parser


def load_treeherder_data(conn, repository, revisions, load_all_after):
    client = thclient.TreeherderClient()
    result_sets = get_result_sets(client, repository, revisions, load_all_after)
    #Process from oldest to newest
    for result_set in reversed(result_sets):
        #First insert any new data
        logger.info("processing result set for revision %s" % result_set['revision'])
        finished_jobs = get_finished_jobs(client, repository, result_set)
        new_jobs = get_new_jobs(conn, finished_jobs)
        error_urls = get_error_urls(client, repository, new_jobs)
        error_lines = get_error_lines(client, error_urls)
        insert_jobs(conn, result_set["id"], new_jobs, error_lines)
    return result_sets


def main():
    global logger
    args = parser().parse_args()
    logger = mozlog.commandline.setup_logging("autostar", args, {"mach": sys.stdout})

    with sqlite3.Connection(args.db_name) as conn:
        if args.setup:
            setup_database(conn, *args.setup)
            return

        repository, last_rev = get_meta(conn)

        if args.repository is not None:
            repository = args.repository
        load_all_after = repository != "try"
        if args.rev:
            revisions = args.revisions
            load_all_after = False
        else:
            revisions = [last_rev]

        result_sets = load_treeherder_data(conn, repository, revisions, load_all_after)

        all_jobs = []
        for result_set in reversed(result_sets):
            jobs = load_jobs(conn, result_set["id"])
            all_jobs.extend(jobs)
            process_errors(conn, jobs)

        add_new_intermittents(conn, all_jobs)

if __name__ == "__main__":
    try:
        main()
    except Exception:
        import pdb
        pdb.post_mortem()
